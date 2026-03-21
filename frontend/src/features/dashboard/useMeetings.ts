import { useMutation, useQuery } from "@tanstack/react-query";

import { apiRequest } from "../../lib/api/client";
import { queryClient } from "../../lib/api/queryClient";
import type { MeetingSummary } from "../../lib/api/types";


type CreateMeetingPayload = {
  title: string;
  teams_link: string;
  audio_recording_enabled: boolean;
};


export function useMeetings() {
  return useQuery({
    queryKey: ["meetings"],
    queryFn: () => apiRequest<MeetingSummary[]>("/api/meetings"),
    refetchInterval: 1000,
  });
}


function invalidateMeetings() {
  return Promise.all([
    queryClient.invalidateQueries({ queryKey: ["meetings"] }),
    queryClient.invalidateQueries({ queryKey: ["meeting-snapshot"] }),
  ]);
}


export function useCreateMeeting() {
  return useMutation({
    mutationFn: async (payload: CreateMeetingPayload) => {
      const created = await apiRequest<MeetingSummary>("/api/meetings", {
        method: "POST",
        body: JSON.stringify(payload),
      });
      return apiRequest<MeetingSummary>(`/api/meetings/${created.id}/join`, {
        method: "POST",
      });
    },
    onSettled: invalidateMeetings,
  });
}


export function useJoinMeeting() {
  return useMutation({
    mutationFn: (meetingId: number) =>
      apiRequest<MeetingSummary>(`/api/meetings/${meetingId}/join`, {
        method: "POST",
      }),
    onSuccess: invalidateMeetings,
  });
}


export function useStopMeeting() {
  return useMutation({
    mutationFn: (meetingId: number) =>
      apiRequest<MeetingSummary>(`/api/meetings/${meetingId}/stop`, {
        method: "POST",
      }),
    onSuccess: invalidateMeetings,
  });
}


export function useDeleteMeeting() {
  return useMutation({
    mutationFn: (meetingId: number) =>
      apiRequest(`/api/meetings/${meetingId}`, {
        method: "DELETE",
      }),
    onSuccess: invalidateMeetings,
  });
}
