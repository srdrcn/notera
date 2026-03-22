type LogLevel = "debug" | "info" | "warn" | "error";

type LogFields = Record<string, unknown>;

const levelOrder: Record<LogLevel, number> = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
};

const baseContext: LogFields = {
  app: "frontend",
  env: import.meta.env.MODE,
};

let initialized = false;

function normalizeLevel(value: string | undefined): LogLevel {
  const candidate = value?.trim().toLowerCase();
  if (candidate === "debug" || candidate === "info" || candidate === "warn" || candidate === "error") {
    return candidate;
  }
  return import.meta.env.PROD ? "info" : "debug";
}

const configuredLevel = normalizeLevel(import.meta.env.VITE_LOG_LEVEL);

function shouldLog(level: LogLevel): boolean {
  return levelOrder[level] >= levelOrder[configuredLevel];
}

function sanitizeValue(value: unknown): unknown {
  if (value == null) {
    return undefined;
  }
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return value;
  }
  if (value instanceof Error) {
    return {
      name: value.name,
      message: value.message,
      stack: value.stack,
    };
  }
  if (Array.isArray(value)) {
    return value.map((item) => sanitizeValue(item));
  }
  if (typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .map(([key, item]) => [key, sanitizeValue(item)])
        .filter((entry) => entry[1] !== undefined),
    );
  }
  return String(value);
}

function currentPath(): string {
  if (typeof window === "undefined") {
    return "/";
  }
  return `${window.location.pathname}${window.location.search}${window.location.hash}`;
}

function emit(level: LogLevel, event: string, message: string, fields: LogFields = {}): void {
  if (!shouldLog(level)) {
    return;
  }

  const payload = Object.fromEntries(
    Object.entries({
      ts: new Date().toISOString(),
      level,
      ...baseContext,
      event,
      message,
      ...fields,
    }).filter((entry) => entry[1] !== undefined),
  );

  const serialized = JSON.stringify(payload);
  if (level === "error") {
    console.error(serialized);
  } else if (level === "warn") {
    console.warn(serialized);
  } else if (level === "debug") {
    console.debug(serialized);
  } else {
    console.info(serialized);
  }
}

export function setLogContext(fields: LogFields): void {
  for (const [key, value] of Object.entries(fields)) {
    const sanitized = sanitizeValue(value);
    if (sanitized === undefined) {
      delete baseContext[key];
      continue;
    }
    baseContext[key] = sanitized;
  }
}

export function describeError(error: unknown): LogFields {
  if (error instanceof Error) {
    return {
      error_name: error.name,
      error_message: error.message,
      error_stack: error.stack,
    };
  }
  return {
    error_name: typeof error,
    error_message: typeof error === "string" ? error : JSON.stringify(sanitizeValue(error)),
  };
}

export function logEvent(level: LogLevel, event: string, message: string, fields: LogFields = {}): void {
  emit(level, event, message, Object.fromEntries(
    Object.entries(fields)
      .map(([key, value]) => [key, sanitizeValue(value)])
      .filter((entry) => entry[1] !== undefined),
  ));
}

export function initializeClientLogging(): void {
  if (initialized) {
    return;
  }
  initialized = true;
  setLogContext({ path: currentPath() });

  window.addEventListener("error", (event) => {
    setLogContext({ path: currentPath() });
    logEvent("error", "window.error", "Unhandled window error", {
      path: currentPath(),
      ...describeError(event.error ?? new Error(event.message)),
    });
  });

  window.addEventListener("unhandledrejection", (event) => {
    setLogContext({ path: currentPath() });
    logEvent("error", "window.unhandledrejection", "Unhandled promise rejection", {
      path: currentPath(),
      ...describeError(event.reason),
    });
  });

  logEvent("info", "app.boot", "Frontend application booted", {
    path: currentPath(),
  });
}
