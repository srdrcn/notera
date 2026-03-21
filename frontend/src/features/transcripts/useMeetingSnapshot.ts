import { useMutation, useQuery } from "@tanstack/react-query";

import { apiRequest } from "../../lib/api/client";
import { queryClient } from "../../lib/api/queryClient";
import type { MeetingSnapshot, MeetingSummary } from "../../lib/api/types";


export function useMeetingSnapshot(meetingId: number) {
  return useQuery({
    queryKey: ["meeting-snapshot", meetingId],
    queryFn: () => apiRequest<MeetingSnapshot>(`/api/meetings/${meetingId}/snapshot`),
    refetchInterval: (query) => {
      const snapshot = query.state.data;
      if (!snapshot) {
        return 1000;
      }
      return ["transcribing", "aligning"].includes(snapshot.postprocess.status) ? 250 : 1000;
    },
  });
}


function invalidateMeeting(meetingId: number) {
  return Promise.all([
    queryClient.invalidateQueries({ queryKey: ["meeting-snapshot", meetingId] }),
    queryClient.invalidateQueries({ queryKey: ["meetings"] }),
  ]);
}


export function useStopTranscriptMeeting(meetingId: number) {
  return useMutation({
    mutationFn: () =>
      apiRequest<MeetingSummary>(`/api/meetings/${meetingId}/stop`, {
        method: "POST",
      }),
    onSuccess: () => invalidateMeeting(meetingId),
  });
}


export function useApplyReview(meetingId: number) {
  return useMutation({
    mutationFn: (reviewId: number) =>
      apiRequest(`/api/reviews/${reviewId}/apply`, {
        method: "POST",
      }),
    onSuccess: () => invalidateMeeting(meetingId),
  });
}


export function useKeepReview(meetingId: number) {
  return useMutation({
    mutationFn: (reviewId: number) =>
      apiRequest(`/api/reviews/${reviewId}/keep`, {
        method: "POST",
      }),
    onSuccess: () => invalidateMeeting(meetingId),
  });
}


export function useApplyAllReviews(meetingId: number) {
  return useMutation({
    mutationFn: () =>
      apiRequest(`/api/meetings/${meetingId}/reviews/apply-all`, {
        method: "POST",
      }),
    onSuccess: () => invalidateMeeting(meetingId),
  });
}


export function useMergeDuplicates(meetingId: number) {
  return useMutation({
    mutationFn: () =>
      apiRequest(`/api/meetings/${meetingId}/transcripts/merge-duplicates`, {
        method: "POST",
      }),
    onSuccess: () => invalidateMeeting(meetingId),
  });
}
