import { describe, expect, it } from "bun:test";
import { status } from "@grpc/grpc-js";
import {
  APIConnectionError,
  APIStatusError,
  AuthenticationError,
  BoxdError,
  ConflictError,
  NotFoundError,
  PermissionDeniedError,
  RateLimitError,
  fromGrpcError,
  isRetryable,
} from "../src/errors.js";

const mk = (code: number, details = "err") =>
  ({ code, details, message: details }) as never;

describe("error hierarchy", () => {
  it("BoxdError is an Error", () => {
    const err = new BoxdError("boom");
    expect(err).toBeInstanceOf(Error);
    expect(err.message).toBe("boom");
    expect(err.name).toBe("BoxdError");
  });

  it("every class descends from BoxdError", () => {
    for (const e of [
      new AuthenticationError("x"),
      new PermissionDeniedError("x"),
      new NotFoundError("x"),
      new ConflictError("x"),
      new RateLimitError("x"),
      new APIStatusError("x"),
      new APIConnectionError("x"),
    ]) {
      expect(e).toBeInstanceOf(BoxdError);
    }
  });
});

describe("fromGrpcError", () => {
  it("UNAUTHENTICATED → AuthenticationError", () =>
    expect(fromGrpcError(mk(status.UNAUTHENTICATED))).toBeInstanceOf(AuthenticationError));
  it("PERMISSION_DENIED → PermissionDeniedError", () =>
    expect(fromGrpcError(mk(status.PERMISSION_DENIED))).toBeInstanceOf(PermissionDeniedError));
  it("NOT_FOUND → NotFoundError", () =>
    expect(fromGrpcError(mk(status.NOT_FOUND))).toBeInstanceOf(NotFoundError));
  it("ALREADY_EXISTS → ConflictError", () =>
    expect(fromGrpcError(mk(status.ALREADY_EXISTS))).toBeInstanceOf(ConflictError));
  it("FAILED_PRECONDITION → ConflictError", () =>
    expect(fromGrpcError(mk(status.FAILED_PRECONDITION))).toBeInstanceOf(ConflictError));
  // e.g. updateSpec when the spec changed while it was applied.
  it("ABORTED → ConflictError", () => {
    const e = fromGrpcError(mk(status.ABORTED, "the machine's spec changed"));
    expect(e).toBeInstanceOf(ConflictError);
    expect(e.grpcCode).toBe(status.ABORTED);
  });
  it("RESOURCE_EXHAUSTED → RateLimitError", () =>
    expect(fromGrpcError(mk(status.RESOURCE_EXHAUSTED))).toBeInstanceOf(RateLimitError));
  it("UNAVAILABLE → APIConnectionError", () =>
    expect(fromGrpcError(mk(status.UNAVAILABLE))).toBeInstanceOf(APIConnectionError));

  it("anything else → APIStatusError", () => {
    for (const c of [
      status.INVALID_ARGUMENT,
      status.INTERNAL,
      status.UNKNOWN,
      status.DEADLINE_EXCEEDED,
      status.DATA_LOSS,
    ]) {
      const e = fromGrpcError(mk(c));
      expect(e).toBeInstanceOf(APIStatusError);
      expect(e).not.toBeInstanceOf(APIConnectionError);
    }
  });

  it("carries the code and the server's detail message", () => {
    const e = fromGrpcError(mk(status.NOT_FOUND, "machine xyz not found"));
    expect(e.message).toBe("machine xyz not found");
    expect(e.grpcCode).toBe(status.NOT_FOUND);
  });
});

describe("retry policy", () => {
  it("connection failures are retryable", () => {
    expect(isRetryable(fromGrpcError(mk(status.UNAVAILABLE)))).toBe(true);
    expect(isRetryable(new APIConnectionError("socket hang up"))).toBe(true);
  });

  it("auth failures are terminal, never retried", () => {
    expect(isRetryable(fromGrpcError(mk(status.UNAUTHENTICATED)))).toBe(false);
    expect(isRetryable(new AuthenticationError("revoked"))).toBe(false);
  });

  it("a timeout is not retried — the call may have been applied", () => {
    expect(isRetryable(fromGrpcError(mk(status.DEADLINE_EXCEEDED)))).toBe(false);
  });

  it("nothing else is retried either", () => {
    for (const c of [status.NOT_FOUND, status.ALREADY_EXISTS, status.ABORTED, status.RESOURCE_EXHAUSTED]) {
      expect(isRetryable(fromGrpcError(mk(c)))).toBe(false);
    }
  });
});
