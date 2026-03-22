import { MutationCache, QueryCache, QueryClient } from "@tanstack/react-query";

import { ApiError } from "./client";
import { describeError, logEvent } from "../logging/logger";


function serializeKey(key: unknown): string {
  try {
    return JSON.stringify(key);
  } catch {
    return String(key);
  }
}


export const queryClient = new QueryClient({
  queryCache: new QueryCache({
    onError: (error, query) => {
      logEvent("error", "query.error", "Query execution failed", {
        query_key: serializeKey(query.queryKey),
        request_id: error instanceof ApiError ? error.requestId : undefined,
        status_code: error instanceof ApiError ? error.status : undefined,
        ...describeError(error),
      });
    },
  }),
  mutationCache: new MutationCache({
    onError: (error, _variables, _context, mutation) => {
      logEvent("error", "mutation.error", "Mutation execution failed", {
        mutation_key: serializeKey(mutation.options.mutationKey ?? mutation.options.meta ?? "anonymous"),
        request_id: error instanceof ApiError ? error.requestId : undefined,
        status_code: error instanceof ApiError ? error.status : undefined,
        ...describeError(error),
      });
    },
  }),
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
    },
  },
});
