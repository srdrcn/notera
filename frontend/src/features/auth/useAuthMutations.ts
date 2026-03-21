import { useMutation } from "@tanstack/react-query";

import { apiRequest } from "../../lib/api/client";
import { queryClient } from "../../lib/api/queryClient";
import type { SessionResponse } from "../../lib/api/types";


type AuthPayload = {
  email: string;
};


export function useRegister() {
  return useMutation({
    mutationFn: (payload: AuthPayload) =>
      apiRequest<SessionResponse>("/api/auth/register", {
        method: "POST",
        body: JSON.stringify(payload),
      }),
    onSuccess: async (response) => {
      queryClient.setQueryData(["session"], response.user);
    },
  });
}


export function useLogin() {
  return useMutation({
    mutationFn: (payload: AuthPayload) =>
      apiRequest<SessionResponse>("/api/auth/login", {
        method: "POST",
        body: JSON.stringify(payload),
      }),
    onSuccess: async (response) => {
      queryClient.setQueryData(["session"], response.user);
    },
  });
}
