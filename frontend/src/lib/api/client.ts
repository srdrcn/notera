import { describeError, logEvent } from "../logging/logger";

export class ApiError extends Error {
  status: number;
  requestId: string | null;
  path: string;
  method: string;

  constructor(
    message: string,
    status: number,
    options: { requestId?: string | null; path: string; method: string },
  ) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.requestId = options.requestId ?? null;
    this.path = options.path;
    this.method = options.method;
  }
}


const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "";


export function buildApiUrl(path: string): string {
  if (!API_BASE_URL) {
    return path;
  }
  return `${API_BASE_URL}${path}`;
}


export async function apiRequest<T = unknown>(
  path: string,
  init: RequestInit = {},
): Promise<T> {
  const method = (init.method ?? "GET").toUpperCase();
  const headers = new Headers(init.headers ?? undefined);
  if (!headers.has("Content-Type") && init.body && !(init.body instanceof FormData)) {
    headers.set("Content-Type", "application/json");
  }

  const startedAt = performance.now();
  logEvent("debug", "http.request.started", "API request started", {
    method,
    path,
  });

  let response: Response;
  try {
    response = await fetch(buildApiUrl(path), {
      ...init,
      headers,
      credentials: "include",
    });
  } catch (error) {
    const durationMs = Number((performance.now() - startedAt).toFixed(2));
    const details = describeError(error);
    logEvent("error", "http.request.failed", "API request failed", {
      method,
      path,
      duration_ms: durationMs,
      ...details,
    });
    throw new ApiError(
      details.error_message ? String(details.error_message) : "Network request failed",
      0,
      { path, method },
    );
  }

  const requestId = response.headers.get("X-Request-ID");

  if (!response.ok) {
    const contentType = response.headers.get("content-type") ?? "";
    let message = `Request failed with status ${response.status}`;

    if (contentType.includes("application/json")) {
      const payload = await response.json();
      if (typeof payload?.detail === "string") {
        message = payload.detail;
      } else if (Array.isArray(payload?.detail) && payload.detail.length > 0) {
        const firstDetail = payload.detail[0];
        if (typeof firstDetail?.msg === "string") {
          message = firstDetail.msg;
        }
      }
    } else {
      const text = await response.text();
      if (text) {
        message = text;
      }
    }

    logEvent(response.status >= 500 ? "error" : "warn", "http.request.failed", "API request failed", {
      method,
      path,
      request_id: requestId,
      status_code: response.status,
      duration_ms: Number((performance.now() - startedAt).toFixed(2)),
      error_name: "ApiError",
      error_message: message,
    });
    throw new ApiError(message, response.status, { requestId, path, method });
  }

  logEvent("info", "http.request.completed", "API request completed", {
    method,
    path,
    request_id: requestId,
    status_code: response.status,
    duration_ms: Number((performance.now() - startedAt).toFixed(2)),
  });

  if (response.status === 204) {
    return undefined as T;
  }

  const contentType = response.headers.get("content-type") ?? "";
  if (!contentType.includes("application/json")) {
    return undefined as T;
  }

  return response.json() as Promise<T>;
}
