import { AuthenticationError, RateLimitError } from "./errors.js";
import type { ReadFile } from "./_platform.js";
import { readEnv } from "./utils.js";

const METADATA_PORT = 9002;
const DEFAULT_GATEWAY = "10.42.0.1";
const METADATA_TIMEOUT_MS = 2_000;

/** Re-exchange an API-key session's token this long before it expires. */
const REFRESH_SKEW_SEC = 300;
/** In-machine credentials are short-lived and re-issued rather than refreshed. */
const VM_REMINT_SKEW_SEC = 60;

/** Which kind of credential the client ended up using. */
export type CredentialSource = "token" | "apiKey" | "vm";

export interface CredentialsOptions {
  /** Explicit token. Used as-is, never refreshed. */
  token?: string;
  /** Explicit API key. Exchanged for a session token and kept fresh. */
  apiKey?: string;
  /** Derived from `baseURL` — see `deriveExchangeUrl`. */
  exchangeUrl: string;
  /**
   * Reads `/proc/net/route` to find the in-machine metadata adapter. Absent on
   * the web entry, which therefore has no automatic in-machine auth.
   */
  readFile?: ReadFile;
}

interface VmIdentity {
  token: string;
  /** Epoch seconds. */
  expiresAt: number;
  /** The cluster this machine belongs to. */
  apiUrl?: string;
}

/** @internal Default route's gateway, or `undefined` off Linux. */
export async function detectGateway(readFile: ReadFile): Promise<string | undefined> {
  let content: string;
  try {
    content = new TextDecoder().decode(await readFile("/proc/net/route"));
  } catch {
    return undefined; // not Linux, or no procfs
  }
  for (const line of content.split("\n").slice(1)) {
    // Iface Destination Gateway Flags RefCnt Use Metric Mask MTU Window IRTT
    const fields = line.trim().split(/\s+/);
    if (fields.length < 3 || fields[1] !== "00000000") continue;
    // Hex-encoded little-endian IPv4.
    const ip = Number.parseInt(fields[2]!, 16);
    if (!Number.isFinite(ip)) return undefined;
    return [ip & 0xff, (ip >> 8) & 0xff, (ip >> 16) & 0xff, (ip >>> 24) & 0xff].join(".");
  }
  return undefined;
}

/**
 * @internal
 * Obtain a credential from inside a boxd machine, or `undefined` when we are
 * not running in one or can't find its metadata adapter (the web entry).
 *
 * The cheap check is only a hint: it skips the probe in the common case of
 * running on a laptop. It is deliberately permissive, because which of these
 * a shell actually has depends on how the session was opened and on how old
 * the machine is — gating on any single one of them silently disables in-VM
 * auth on machines that are otherwise perfectly fine. The probe below is the
 * real test: nothing outside a boxd machine answers it.
 */
const VM_HINTS = ["BOXD_VM_ID", "BOXD_VM_NAME", "BOXD_PROXY_ENDPOINT"] as const;

/**
 * @internal
 * The in-machine metadata adapter's base URL, or `undefined` when this code
 * isn't running inside a boxd machine, or can't find the adapter (the web
 * entry has no file reader for the route table).
 */
export async function metadataBase(readFile: ReadFile | undefined): Promise<string | undefined> {
  if (!VM_HINTS.some((v) => readEnv(v))) return undefined;
  if (!readFile) return undefined;
  const gateway = await detectGateway(readFile);
  return gateway ? `http://${gateway}:${METADATA_PORT}` : undefined;
}

export async function mintVmToken(
  readFile: ReadFile | undefined,
): Promise<VmIdentity | undefined> {
  if (!VM_HINTS.some((v) => readEnv(v))) return undefined;
  // The metadata adapter listens on the worker's bridge gateway, which differs
  // per worker (`10.<n>.0.1`), so without a file reader to read the route
  // table (the web entry) there is no way to find it. `DEFAULT_GATEWAY` only
  // matches a local dev cluster, so guessing it would just time out.
  if (!readFile) return undefined;

  const gateway = (await detectGateway(readFile)) ?? DEFAULT_GATEWAY;

  try {
    const resp = await fetch(`http://${gateway}:${METADATA_PORT}/v1/auth/token`, {
      method: "GET",
      signal: AbortSignal.timeout(METADATA_TIMEOUT_MS),
    });
    if (!resp.ok) return undefined;
    const data = (await resp.json()) as {
      token?: unknown;
      expires_at?: unknown;
      api_url?: unknown;
    };
    if (typeof data.token !== "string" || typeof data.expires_at !== "number") {
      return undefined;
    }
    return {
      token: data.token,
      expiresAt: data.expires_at,
      apiUrl: typeof data.api_url === "string" && data.api_url ? data.api_url : undefined,
    };
  } catch {
    // Unreachable, timed out, or not JSON — all mean "not a boxd machine".
    // Fall through to the next rung of the resolution chain.
    return undefined;
  }
}

/**
 * The credential half of the client: decides *which* credential to use, then
 * keeps it fresh.
 *
 * Resolution is deferred to the first request, because deciding can require
 * I/O. The decision is made once and sticks. Order, first match wins:
 *
 *  1. explicit `token`
 *  2. explicit `apiKey`
 *  3. `BOXD_TOKEN`
 *  4. `BOXD_API_KEY`
 *  5. automatic, when running inside a boxd machine
 *  6. otherwise `AuthenticationError`
 */
export class Credentials {
  readonly #explicitToken?: string;
  readonly #explicitApiKey?: string;
  readonly #envToken?: string;
  readonly #envApiKey?: string;
  readonly #exchangeUrl: string;
  readonly #readFile: ReadFile | undefined;

  #resolving?: Promise<CredentialSource>;
  /** The cluster URL a boxd machine reported, when that rung won. */
  #vmApiUrl?: string;

  #apiKey?: string;
  #token?: string;
  /** Epoch seconds. 0 for a never-expiring explicit token. */
  #expiresAt = 0;
  #inflight?: Promise<void>;

  constructor(opts: CredentialsOptions) {
    this.#explicitToken = opts.token || undefined;
    this.#explicitApiKey = opts.apiKey || undefined;
    this.#envToken = readEnv("BOXD_TOKEN") || undefined;
    this.#envApiKey = readEnv("BOXD_API_KEY") || undefined;
    this.#exchangeUrl = opts.exchangeUrl;
    this.#readFile = opts.readFile;
  }

  /** Which rung won. Resolves the chain on first call. */
  async source(): Promise<CredentialSource> {
    this.#resolving ??= this.#resolve();
    return this.#resolving;
  }

  /**
   * The cluster URL reported by the surrounding boxd machine, if any. An
   * explicit `baseURL` still takes precedence — the client decides.
   */
  async clusterUrl(): Promise<string | undefined> {
    await this.source();
    return this.#vmApiUrl;
  }

  async getToken(): Promise<string> {
    const source = await this.source();
    if (source === "token") return this.#token!;

    const now = Date.now() / 1000;
    const skew = source === "vm" ? VM_REMINT_SKEW_SEC : REFRESH_SKEW_SEC;
    if (!this.#token || now > this.#expiresAt - skew) {
      // Coalesce: N concurrent calls queue behind one refresh.
      this.#inflight ??= (source === "vm" ? this.#remint() : this.#exchange()).finally(() => {
        this.#inflight = undefined;
      });
      await this.#inflight;
    }
    return this.#token!;
  }

  async #resolve(): Promise<CredentialSource> {
    if (this.#explicitToken) {
      this.#token = this.#explicitToken;
      return "token";
    }
    if (this.#explicitApiKey) {
      this.#apiKey = this.#explicitApiKey;
      return "apiKey";
    }
    if (this.#envToken) {
      this.#token = this.#envToken;
      return "token";
    }
    if (this.#envApiKey) {
      this.#apiKey = this.#envApiKey;
      return "apiKey";
    }
    const vm = await mintVmToken(this.#readFile);
    if (vm) {
      this.#token = vm.token;
      this.#expiresAt = vm.expiresAt;
      this.#vmApiUrl = vm.apiUrl;
      return "vm";
    }
    // Without a file reader (the web entry) in-machine auth never runs, so
    // don't imply it would.
    const inMachine = this.#readFile
      ? "authentication is automatic only inside a boxd machine"
      : "automatic authentication inside a boxd machine needs the Node entry, @boxd-sh/sdk";
    throw new AuthenticationError(
      "no credentials: pass { apiKey } or { token }, or set BOXD_API_KEY / BOXD_TOKEN " +
        `(${inMachine})`,
    );
  }

  async #exchange(): Promise<void> {
    // The token-exchange endpoint is rate-limited per source IP. A fleet of
    // workers behind one NAT can trip it even with legitimate traffic, so on a
    // 429 wait out the server's `Retry-After` once before surfacing the error.
    for (let attempt = 0; ; attempt++) {
      const resp = await fetch(this.#exchangeUrl, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ api_key: this.#apiKey }),
      });

      if (resp.status === 401) {
        throw new AuthenticationError("invalid or expired API key");
      }
      if (resp.status === 429) {
        const body = (await resp.text().catch(() => "")) || "token exchange rate limit exceeded";
        if (attempt === 0) {
          const raw = resp.headers.get("retry-after");
          const parsed = raw == null ? Number.NaN : Number(raw);
          const waitSec = Number.isFinite(parsed) && parsed >= 0 ? Math.min(parsed, 60) : 5;
          await sleep(waitSec * 1000);
          continue;
        }
        throw new RateLimitError(
          `${body} — a token is valid for 1 hour; reuse one across your workers`,
        );
      }
      if (!resp.ok) {
        const body = await resp.text().catch(() => "");
        throw new AuthenticationError(`could not exchange API key: ${resp.status} ${body}`);
      }

      const data = (await resp.json()) as { token?: unknown; expires_at?: unknown };
      if (typeof data.token !== "string" || typeof data.expires_at !== "number") {
        throw new AuthenticationError("malformed response while exchanging API key");
      }
      this.#token = data.token;
      this.#expiresAt = data.expires_at;
      return;
    }
  }

  /** In-machine credentials are re-issued, not refreshed. */
  async #remint(): Promise<void> {
    const vm = await mintVmToken(this.#readFile);
    if (!vm) {
      throw new AuthenticationError(
        "the machine's automatic credential expired and could not be renewed",
      );
    }
    this.#token = vm.token;
    this.#expiresAt = vm.expiresAt;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}
