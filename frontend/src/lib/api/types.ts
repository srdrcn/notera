export type User = {
  id: number;
  email: string;
};

export type SessionResponse = {
  user: User;
};

export type MeetingSummary = {
  id: number;
  title: string;
  status: string;
  audio_status: string;
  postprocess_status: string;
  postprocess_progress_pct: number | null;
  postprocess_progress_note: string | null;
  created_at: string | null;
  joined_at: string | null;
  ended_at: string | null;
  can_join: boolean;
  can_stop: boolean;
  can_view_transcripts: boolean;
};

export type Review = {
  id: number;
  review_type: string;
  confidence_label: string;
  current_text: string;
  suggested_text: string;
  current_participant_id: number | null;
  suggested_participant_id: number | null;
  audio_clip_url: string | null;
  has_audio_clip: boolean;
};

export type ParticipantEntry = {
  id: number;
  display_name: string;
  binding_state: string;
  segment_count: number;
  has_audio_asset: boolean;
  is_bot: boolean;
  join_state: string;
};

export type SegmentEntry = {
  id: number;
  participant_id: number | null;
  speaker: string;
  text: string;
  raw_text: string;
  timestamp: string;
  start_sec: number | null;
  end_sec: number | null;
  initials: string;
  color: string;
  assignment_method: string;
  assignment_confidence: number;
  needs_speaker_review: boolean;
  overlap_group_id: string | null;
  resolution_status: string;
  review: Review | null;
};

export type MeetingSnapshot = {
  meeting: {
    id: number;
    title: string;
    status: string;
  };
  summary: {
    speaker_count: number;
    segment_count: number;
    pending_speaker_review_count: number;
  };
  audio: {
    status: string;
    error: string | null;
    has_audio: boolean;
    audio_url: string | null;
    label: string;
  };
  postprocess: {
    status: string;
    error: string | null;
    progress_pct: number | null;
    progress_note: string | null;
  };
  preview: {
    has_preview: boolean;
    image_url: string | null;
    label: string;
  };
  participants: ParticipantEntry[];
  segments: SegmentEntry[];
  actions: {
    pending_review_count: number;
    can_stop_meeting: boolean;
    can_manage_speakers: boolean;
  };
};
