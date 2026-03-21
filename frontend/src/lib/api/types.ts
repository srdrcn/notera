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
  granularity: string;
  confidence_label: string;
  current_text: string;
  suggested_text: string;
  audio_clip_url: string | null;
  has_audio_clip: boolean;
};

export type TranscriptEntry = {
  id: number;
  speaker: string;
  text: string;
  teams_text: string;
  timestamp: string;
  initials: string;
  color: string;
  resolution_status: string;
  auto_corrected: boolean;
  has_pending_review: boolean;
  has_duplicate_merge_candidate: boolean;
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
    transcript_count: number;
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
  transcripts: TranscriptEntry[];
  actions: {
    pending_review_count: number;
    duplicate_merge_candidate_count: number;
    can_apply_all_reviews: boolean;
    can_merge_duplicate_transcripts: boolean;
    can_stop_meeting: boolean;
  };
};
