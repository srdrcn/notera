import { useMutation } from "@tanstack/react-query";

import { useSession } from "../../app/session";
import { apiRequest } from "../../lib/api/client";
import type { SessionResponse } from "../../lib/api/types";


type AuthPayload = {
  email: string;
};


export function useRegister() {
  const session = useSession();
  return useMutation({
    mutationFn: (payload: AuthPayload) =>
      apiRequest<SessionResponse>("/api/auth/register", {
        method: "POST",
        body: JSON.stringify(payload),
      }),
    onSuccess: async () => {
      await session.refresh();
    },
  });
}


export function useLogin() {
  const session = useSession();
  return useMutation({
    mutationFn: (payload: AuthPayload) =>
      apiRequest<SessionResponse>("/api/auth/login", {
        method: "POST",
        body: JSON.stringify(payload),
      }),
    onSuccess: async () => {
      await session.refresh();
    },
  });
}
