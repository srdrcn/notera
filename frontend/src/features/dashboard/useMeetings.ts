import { useMutation, useQuery } from "@tanstack/react-query";

import { apiRequest } from "../../lib/api/client";
import { queryClient } from "../../lib/api/queryClient";
import type { MeetingSummary } from "../../lib/api/types";


type CreateMeetingPayload = {
  title: string;
  teams_link: string;
  audio_recording_enabled: boolean;
};

const STABLE_POSTPROCESS_STATUSES = new Set(["completed", "review_ready", "failed"]);


function meetingsRefetchInterval(meetings: MeetingSummary[] | undefined): number {
  if (!meetings || meetings.length === 0) {
    return 10_000;
  }

  if (meetings.some((meeting) => ["joining", "active"].includes(meeting.status))) {
    return 3_000;
  }

  const allMeetingsStable = meetings.every(
    (meeting) => meeting.status === "completed" && STABLE_POSTPROCESS_STATUSES.has(meeting.postprocess_status),
  );
  if (allMeetingsStable) {
    return 10_000;
  }

  return 5_000;
}


export function useMeetings() {
  return useQuery({
    queryKey: ["meetings"],
    queryFn: () => apiRequest<MeetingSummary[]>("/api/meetings"),
    refetchInterval: (query) => meetingsRefetchInterval(query.state.data),
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
