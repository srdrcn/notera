import {
  createContext,
  useContext,
  type PropsWithChildren,
} from "react";
import { useQuery } from "@tanstack/react-query";

import { apiRequest } from "../lib/api/client";
import { queryClient } from "../lib/api/queryClient";
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
  } catch {
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
