import { useMutation, useQuery } from "@tanstack/react-query";

import { apiRequest } from "../../lib/api/client";
import { queryClient } from "../../lib/api/queryClient";
import type { MeetingSnapshot, MeetingSummary } from "../../lib/api/types";

const ACTIVE_MEETING_STATUSES = new Set(["joining", "active"]);
const ACTIVE_POSTPROCESS_STATUSES = new Set(["transcribing", "aligning"]);
const STABLE_TRANSCRIPT_STATUSES = new Set(["review_ready", "completed", "failed"]);


function meetingSnapshotRefetchInterval(snapshot: MeetingSnapshot | undefined): number | false {
  if (!snapshot) {
    return 1_500;
  }

  if (ACTIVE_MEETING_STATUSES.has(snapshot.meeting.status)) {
    return 1_500;
  }

  if (ACTIVE_POSTPROCESS_STATUSES.has(snapshot.postprocess.status)) {
    return 1_500;
  }

  if (STABLE_TRANSCRIPT_STATUSES.has(snapshot.postprocess.status)) {
    return false;
  }

  return 5_000;
}


export function useMeetingSnapshot(meetingId: number) {
  return useQuery({
    queryKey: ["meeting-snapshot", meetingId],
    queryFn: () => apiRequest<MeetingSnapshot>(`/api/meetings/${meetingId}/snapshot`),
    refetchInterval: (query) => meetingSnapshotRefetchInterval(query.state.data),
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
