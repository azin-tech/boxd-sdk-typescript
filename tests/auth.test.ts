import { afterEach, describe, expect, it, mock } from "bun:test";
import { nodePlatform } from "../src/_platformNode.js";
import { Credentials, detectGateway } from "../src/auth.js";
import { AuthenticationError, RateLimitError } from "../src/errors.js";

const originalFetch = globalThis.fetch;
const ENV_KEYS = ["BOXD_TOKEN", "BOXD_API_KEY", "BOXD_VM_ID", "BOXD_IDENTITY"] as const;

function clearEnv(): void {
  for (const k of ENV_KEYS) delete process.env[k];
}

const EXCHANGE = "https://app.boxd.sh/api/v1/auth/token";

/** A fetch that answers the exchange endpoint and nothing else. */
function exchangeOk(token = "jwt-1", ttl = 3600) {
  return mock(async () =>
    new Response(JSON.stringify({ token, expires_at: Date.now() / 1000 + ttl }), {
      status: 200,
      headers: { "content-type": "application/json" },
    }),
  );
}

describe("credential resolution order", () => {
  afterEach(() => {
    globalThis.fetch = originalFetch;
    clearEnv();
  });

  it("explicit token wins and is never exchanged", async () => {
    const fetchMock = exchangeOk();
    globalThis.fetch = fetchMock as never;
    process.env.BOXD_API_KEY = "bxd_env";
    const creds = new Credentials({ token: "fixed", apiKey: "bxd_x", exchangeUrl: EXCHANGE });
    expect(await creds.source()).toBe("token");
    expect(await creds.getToken()).toBe("fixed");
    expect(await creds.getToken()).toBe("fixed");
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("explicit apiKey beats BOXD_TOKEN", async () => {
    globalThis.fetch = exchangeOk("from-key") as never;
    process.env.BOXD_TOKEN = "env-token";
    const creds = new Credentials({ apiKey: "bxd_x", exchangeUrl: EXCHANGE });
    expect(await creds.source()).toBe("apiKey");
    expect(await creds.getToken()).toBe("from-key");
  });

  it("BOXD_TOKEN beats BOXD_API_KEY", async () => {
    const fetchMock = exchangeOk();
    globalThis.fetch = fetchMock as never;
    process.env.BOXD_TOKEN = "env-token";
    process.env.BOXD_API_KEY = "bxd_env";
    const creds = new Credentials({ exchangeUrl: EXCHANGE });
    expect(await creds.source()).toBe("token");
    expect(await creds.getToken()).toBe("env-token");
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("BOXD_API_KEY is the last credentialed rung", async () => {
    globalThis.fetch = exchangeOk("from-env-key") as never;
    process.env.BOXD_API_KEY = "bxd_env";
    const creds = new Credentials({ exchangeUrl: EXCHANGE });
    expect(await creds.source()).toBe("apiKey");
    expect(await creds.getToken()).toBe("from-env-key");
  });

  it("no credentials and not in a machine → AuthenticationError", async () => {
    globalThis.fetch = mock(async () => new Response("", { status: 404 })) as never;
    const creds = new Credentials({ exchangeUrl: EXCHANGE });
    await expect(creds.source()).rejects.toBeInstanceOf(AuthenticationError);
  });

  it("BOXD_VM_ID alone is not enough — the environment must also answer", async () => {
    process.env.BOXD_VM_ID = "vm-123";
    // Nothing listening: every probe fails.
    globalThis.fetch = mock(async () => {
      throw new Error("ECONNREFUSED");
    }) as never;
    const creds = new Credentials({ exchangeUrl: EXCHANGE, readFile: nodePlatform.readFile });
    await expect(creds.source()).rejects.toBeInstanceOf(AuthenticationError);
  });

  it("in a machine without a file reader (the web entry) → names the Node entry, no probe", async () => {
    process.env.BOXD_VM_ID = "vm-123";
    const fetchMock = mock(async () => new Response("", { status: 404 }));
    globalThis.fetch = fetchMock as never;
    const creds = new Credentials({ exchangeUrl: EXCHANGE });
    await expect(creds.source()).rejects.toThrow(AuthenticationError);
    await expect(creds.source()).rejects.toThrow(/inside a boxd machine needs the Node entry/);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("in-machine auth needs no configuration and reports its own cluster", async () => {
    process.env.BOXD_VM_ID = "vm-123";
    globalThis.fetch = mock(async () => {
      return new Response(
        JSON.stringify({
          token: "vm-jwt",
          expires_at: Date.now() / 1000 + 900,
          api_url: "http://staging.example.com:9443",
        }),
        { status: 200, headers: { "content-type": "application/json" } },
      );
    }) as never;

    const creds = new Credentials({ exchangeUrl: EXCHANGE, readFile: nodePlatform.readFile });
    expect(await creds.source()).toBe("vm");
    expect(await creds.getToken()).toBe("vm-jwt");
    expect(await creds.clusterUrl()).toBe("http://staging.example.com:9443");
    // The session identity is relayed when present.
  });

  it("resolution happens once, not per call", async () => {
    process.env.BOXD_TOKEN = "env-token";
    const creds = new Credentials({ exchangeUrl: EXCHANGE });
    const [a, b] = await Promise.all([creds.source(), creds.source()]);
    expect(a).toBe("token");
    expect(b).toBe("token");
  });
});

describe("API-key exchange", () => {
  afterEach(() => {
    globalThis.fetch = originalFetch;
    clearEnv();
  });

  it("POSTs the key to the derived exchange URL", async () => {
    const fetchMock = exchangeOk();
    globalThis.fetch = fetchMock as never;
    const creds = new Credentials({ apiKey: "bxd_abc", exchangeUrl: EXCHANGE });
    expect(await creds.getToken()).toBe("jwt-1");
    const call = fetchMock.mock.calls[0]!;
    expect(call[0]).toBe(EXCHANGE);
    expect((call[1] as RequestInit).method).toBe("POST");
    expect(JSON.parse((call[1] as RequestInit).body as string)).toEqual({ api_key: "bxd_abc" });
  });

  it("401 → AuthenticationError", async () => {
    globalThis.fetch = mock(async () => new Response("", { status: 401 })) as never;
    const creds = new Credentials({ apiKey: "bad", exchangeUrl: EXCHANGE });
    await expect(creds.getToken()).rejects.toBeInstanceOf(AuthenticationError);
  });

  it("429 twice → RateLimitError, after one Retry-After wait", async () => {
    const fetchMock = mock(
      async () => new Response("", { status: 429, headers: { "retry-after": "0" } }),
    );
    globalThis.fetch = fetchMock as never;
    const creds = new Credentials({ apiKey: "x", exchangeUrl: EXCHANGE });
    await expect(creds.getToken()).rejects.toBeInstanceOf(RateLimitError);
    // One retry: the initial call plus one after honouring Retry-After.
    expect(fetchMock.mock.calls.length).toBe(2);
  });

  it("429 then 200 → recovers without throwing", async () => {
    let calls = 0;
    globalThis.fetch = mock(async () => {
      calls++;
      if (calls === 1) {
        return new Response("", { status: 429, headers: { "retry-after": "0" } });
      }
      return new Response(
        JSON.stringify({ token: "jwt-after-429", expires_at: Date.now() / 1000 + 3600 }),
        { status: 200, headers: { "content-type": "application/json" } },
      );
    }) as never;
    const creds = new Credentials({ apiKey: "x", exchangeUrl: EXCHANGE });
    expect(await creds.getToken()).toBe("jwt-after-429");
  });

  it("malformed response → AuthenticationError", async () => {
    globalThis.fetch = mock(
      async () =>
        new Response(JSON.stringify({ token: "jwt" }), {
          status: 200,
          headers: { "content-type": "application/json" },
        }),
    ) as never;
    const creds = new Credentials({ apiKey: "x", exchangeUrl: EXCHANGE });
    await expect(creds.getToken()).rejects.toBeInstanceOf(AuthenticationError);
  });

  it("re-exchanges inside the refresh window", async () => {
    let calls = 0;
    globalThis.fetch = mock(async () => {
      calls++;
      // First token expires in 100s — inside the 300s refresh skew.
      const ttl = calls === 1 ? 100 : 3600;
      return new Response(
        JSON.stringify({ token: `jwt-${calls}`, expires_at: Date.now() / 1000 + ttl }),
        { status: 200, headers: { "content-type": "application/json" } },
      );
    }) as never;
    const creds = new Credentials({ apiKey: "x", exchangeUrl: EXCHANGE });
    expect(await creds.getToken()).toBe("jwt-1");
    expect(await creds.getToken()).toBe("jwt-2");
    expect(calls).toBe(2);
  });

  it("coalesces concurrent refreshes into one exchange", async () => {
    let calls = 0;
    globalThis.fetch = mock(async () => {
      calls++;
      await new Promise((r) => setTimeout(r, 10));
      return new Response(
        JSON.stringify({ token: "jwt-1", expires_at: Date.now() / 1000 + 3600 }),
        { status: 200, headers: { "content-type": "application/json" } },
      );
    }) as never;
    const creds = new Credentials({ apiKey: "x", exchangeUrl: EXCHANGE });
    const results = await Promise.all([
      creds.getToken(),
      creds.getToken(),
      creds.getToken(),
      creds.getToken(),
    ]);
    expect(results).toEqual(["jwt-1", "jwt-1", "jwt-1", "jwt-1"]);
    expect(calls).toBe(1);
  });
});

describe("detectGateway", () => {
  it("returns a dotted quad, or nothing where there is no route table", async () => {
    const gw = await detectGateway(nodePlatform.readFile);
    if (gw !== undefined) expect(gw).toMatch(/^\d{1,3}(\.\d{1,3}){3}$/);
  });
});
