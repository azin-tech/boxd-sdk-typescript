/**
 * Standard gRPC status codes (the numbers the spec defines — identical to
 * `@grpc/grpc-js`'s `status` enum, reproduced here rather than imported so
 * this file has no grpc-js dependency: it's shared by both transports, and
 * the web transport (`@boxd-sh/sdk/web`) must never pull grpc-js in
 * — that import fails to load in a Cloudflare Worker.
 */
export const GrpcStatus = {
  OK: 0,
  CANCELLED: 1,
  UNKNOWN: 2,
  INVALID_ARGUMENT: 3,
  DEADLINE_EXCEEDED: 4,
  NOT_FOUND: 5,
  ALREADY_EXISTS: 6,
  PERMISSION_DENIED: 7,
  RESOURCE_EXHAUSTED: 8,
  FAILED_PRECONDITION: 9,
  ABORTED: 10,
  OUT_OF_RANGE: 11,
  UNIMPLEMENTED: 12,
  INTERNAL: 13,
  UNAVAILABLE: 14,
  DATA_LOSS: 15,
  UNAUTHENTICATED: 16,
} as const;

/** The bits of grpc-js's `ServiceError` this module actually reads —
 *  reproduced locally (see `GrpcStatus`) so this file stays grpc-js-free. */
export interface RpcError {
  code?: number;
  details?: string;
  message: string;
}

/** Base class for everything the SDK throws. */
export class BoxdError extends Error {
  /** gRPC status code, when the error came off the wire. */
  readonly grpcCode?: number;
  constructor(message: string, grpcCode?: number) {
    super(message);
    this.name = "BoxdError";
    this.grpcCode = grpcCode;
  }
}

/**
 * The credential was rejected, or none could be found.
 *
 * **Terminal.** Revoking a key takes effect immediately, so an
 * `AuthenticationError` part-way through a session means the credential is no
 * longer valid — the client never refreshes and retries, which would only
 * spin.
 */
export class AuthenticationError extends BoxdError {
  constructor(message: string, grpcCode?: number) {
    super(message, grpcCode);
    this.name = "AuthenticationError";
  }
}

/** `permission_denied` — authenticated, but not allowed to touch this. */
export class PermissionDeniedError extends BoxdError {
  constructor(message: string, grpcCode?: number) {
    super(message, grpcCode);
    this.name = "PermissionDeniedError";
  }
}

/** `not_found`. */
export class NotFoundError extends BoxdError {
  constructor(message: string, grpcCode?: number) {
    super(message, grpcCode);
    this.name = "NotFoundError";
  }
}

/**
 * `already_exists` / `failed_precondition` / `aborted` — the state is wrong
 * for this call, or it changed while the call ran (e.g. a machine's spec
 * changed during `updateSpec`, which then changes nothing: read it again and
 * retry).
 */
export class ConflictError extends BoxdError {
  constructor(message: string, grpcCode?: number) {
    super(message, grpcCode);
    this.name = "ConflictError";
  }
}

/** `resource_exhausted` — quota or rate limit. */
export class RateLimitError extends BoxdError {
  constructor(message: string, grpcCode?: number) {
    super(message, grpcCode);
    this.name = "RateLimitError";
  }
}

/** Any other non-OK gRPC status. */
export class APIStatusError extends BoxdError {
  constructor(message: string, grpcCode?: number) {
    super(message, grpcCode);
    this.name = "APIStatusError";
  }
}

/**
 * Transport failure — the request never got a status back, or the server was
 * `unavailable`. The only class the client retries.
 */
export class APIConnectionError extends BoxdError {
  constructor(message: string, grpcCode?: number) {
    super(message, grpcCode);
    this.name = "APIConnectionError";
  }
}

const STATUS_MAP: Record<number, new (msg: string, code?: number) => BoxdError> = {
  [GrpcStatus.UNAUTHENTICATED]: AuthenticationError,
  [GrpcStatus.PERMISSION_DENIED]: PermissionDeniedError,
  [GrpcStatus.NOT_FOUND]: NotFoundError,
  [GrpcStatus.ALREADY_EXISTS]: ConflictError,
  [GrpcStatus.FAILED_PRECONDITION]: ConflictError,
  [GrpcStatus.ABORTED]: ConflictError,
  [GrpcStatus.RESOURCE_EXHAUSTED]: RateLimitError,
  // UNAVAILABLE is a transport failure wearing a status code: the channel
  // never reached a healthy server. Mapping it here (rather than to
  // APIStatusError) is what makes `isRetryable` a single instanceof check.
  [GrpcStatus.UNAVAILABLE]: APIConnectionError,
};

export function fromGrpcError(err: Pick<RpcError, "code" | "details" | "message">): BoxdError {
  const code = err.code;
  const cls = (code !== undefined ? STATUS_MAP[code] : undefined) ?? APIStatusError;
  const msg = err.details || err.message || "grpc error";
  return new cls(msg, code);
}

/**
 * Retry policy: connection failures only.
 *
 * Notably not a timeout — the request may have been applied, and the client
 * cannot tell a read from a write at this layer. Notably not an
 * {@link AuthenticationError} either.
 */
export function isRetryable(err: unknown): boolean {
  return err instanceof APIConnectionError;
}
