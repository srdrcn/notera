import { useMutation, useQuery } from "@tanstack/react-query";

import { apiRequest } from "../../lib/api/client";
import { queryClient } from "../../lib/api/queryClient";
import type { MeetingSnapshot, MeetingSummary } from "../../lib/api/types";

const ACTIVE_MEETING_STATUSES = new Set(["joining", "active"]);
const ACTIVE_POSTPROCESS_STATUSES = new Set([
  "binding_sources",
  "materializing_audio",
  "transcribing_participants",
  "assembling_segments",
  "transcribing",
  "aligning",
  "canonicalizing",
  "rebuilding",
]);
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

export function useUpdateSegmentParticipant(meetingId: number) {
  return useMutation({
    mutationFn: ({ segmentId, participantId }: { segmentId: number; participantId: number | null }) =>
      apiRequest(`/api/transcript-segments/${segmentId}/participant`, {
        method: "PATCH",
        body: JSON.stringify({ participant_id: participantId }),
      }),
    onSuccess: () => invalidateMeeting(meetingId),
  });
}

export function useMergeParticipants(meetingId: number) {
  return useMutation({
    mutationFn: ({ sourceParticipantId, targetParticipantId }: { sourceParticipantId: number; targetParticipantId: number }) =>
      apiRequest(`/api/meetings/${meetingId}/participants/merge`, {
        method: "POST",
        body: JSON.stringify({
          source_participant_id: sourceParticipantId,
          target_participant_id: targetParticipantId,
        }),
      }),
    onSuccess: () => invalidateMeeting(meetingId),
  });
}

export function useSplitParticipant(meetingId: number) {
  return useMutation({
    mutationFn: ({
      participantId,
      segmentIds,
      displayName,
    }: {
      participantId: number;
      segmentIds: number[];
      displayName: string;
    }) =>
      apiRequest(`/api/meetings/${meetingId}/participants/split`, {
        method: "POST",
        body: JSON.stringify({
          participant_id: participantId,
          segment_ids: segmentIds,
          display_name: displayName,
        }),
      }),
    onSuccess: () => invalidateMeeting(meetingId),
  });
}
