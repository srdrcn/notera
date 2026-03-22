import {
  createContext,
  useContext,
  useEffect,
  type PropsWithChildren,
} from "react";
import { useQuery } from "@tanstack/react-query";

import { ApiError, apiRequest } from "../lib/api/client";
import { queryClient } from "../lib/api/queryClient";
import { logEvent, setLogContext } from "../lib/logging/logger";
import type { SessionResponse, User } from "../lib/api/types";


type SessionContextValue = {
  user: User | null;
  isLoading: boolean;
  isAuthenticated: boolean;
  refresh: () => Promise<User | null>;
  logout: () => Promise<void>;
};

const SessionContext = createContext<SessionContextValue | null>(null);


async function fetchSession(): Promise<User | null> {
  try {
    const session = await apiRequest<SessionResponse>("/api/auth/me");
    return session.user;
  } catch (error) {
    if (error instanceof ApiError && error.status === 401) {
      logEvent("info", "auth.session.anonymous", "No active session found", {
        status_code: error.status,
        request_id: error.requestId,
        path: error.path,
        method: error.method,
      });
      return null;
    }
    logEvent("warn", "auth.session.failed", "Session bootstrap failed", {
      ...(error instanceof ApiError
        ? {
            status_code: error.status,
            request_id: error.requestId,
            path: error.path,
            method: error.method,
          }
        : {}),
      error_name: error instanceof Error ? error.name : typeof error,
      error_message: error instanceof Error ? error.message : String(error),
    });
    return null;
  }
}


export function SessionProvider({ children }: PropsWithChildren) {
  const query = useQuery({
    queryKey: ["session"],
    queryFn: fetchSession,
    staleTime: 30_000,
    retry: false,
  });
  const user = query.data ?? null;

  useEffect(() => {
    setLogContext({ user_id: user?.id, path: window.location.pathname + window.location.search + window.location.hash });
  }, [user]);

  async function refresh() {
    const nextUser = await queryClient.fetchQuery({
      queryKey: ["session"],
      queryFn: fetchSession,
      staleTime: 0,
    });
    return nextUser;
  }

  async function logout() {
    await apiRequest("/api/auth/logout", { method: "POST" });
    queryClient.setQueryData(["session"], null);
    await queryClient.invalidateQueries({ queryKey: ["session"] });
  }

  return (
    <SessionContext.Provider
      value={{
        user,
        isLoading: query.isPending,
        isAuthenticated: Boolean(user),
        refresh,
        logout,
      }}
    >
      {children}
    </SessionContext.Provider>
  );
}


export function useSession() {
  const context = useContext(SessionContext);
  if (!context) {
    throw new Error("useSession must be used inside SessionProvider");
  }
  return context;
}
