import { useEffect, useRef, useState } from "react";
import { Link, useParams } from "react-router-dom";

import { AudioPlayer } from "../../components/AudioPlayer";
import { AppShell } from "../../components/AppShell";
import { LoadingView } from "../../components/LoadingView";
import { StatusPill } from "../../components/StatusPill";
import { buildApiUrl } from "../../lib/api/client";
import type { ParticipantEntry, SegmentEntry } from "../../lib/api/types";
import {
  useMeetingSnapshot,
  useMergeParticipants,
  useSplitParticipant,
  useStopTranscriptMeeting,
  useUpdateSegmentParticipant,
} from "./useMeetingSnapshot";

function toneForStatus(status: string): "default" | "success" | "warning" | "danger" | "teal" | "primary" {
  if (status === "completed" || status === "review_ready") {
    return "success";
  }
  if (status === "failed") {
    return "danger";
  }
  if (["joining", "active"].includes(status)) {
    return "primary";
  }
  if (["transcribing_participants", "binding_sources", "assembling_segments"].includes(status)) {
    return "teal";
  }
  if (["queued", "pending", "materializing_audio"].includes(status)) {
    return "warning";
  }
  return "default";
}

function toneForBindingState(bindingState: string): "default" | "success" | "warning" | "danger" | "teal" | "primary" {
  if (bindingState === "confirmed") {
    return "success";
  }
  if (bindingState === "provisional") {
    return "warning";
  }
  return "default";
}

function labelForBindingState(bindingState: string) {
  if (bindingState === "confirmed") {
    return "Hazır";
  }
  if (bindingState === "provisional") {
    return "Kontrol et";
  }
  return "Henüz net değil";
}

function helperTextForParticipant(participant: ParticipantEntry) {
  if (participant.join_state === "left") {
    return "Toplantıdan ayrıldı.";
  }
  if (participant.binding_state === "confirmed") {
    return "Konuşmaları bu kişiyle eşleşti.";
  }
  if (participant.binding_state === "provisional") {
    return "Bazı konuşmalar için kısa bir kontrol gerekebilir.";
  }
  return "Bu kişi için henüz net bir konuşma eşleşmesi yok.";
}

function initialsForParticipant(name: string) {
  const parts = name.trim().split(/\s+/).filter(Boolean);
  if (parts.length === 0) {
    return "?";
  }
  if (parts.length === 1) {
    return parts[0].slice(0, 2).toUpperCase();
  }
  return `${parts[0][0]}${parts[parts.length - 1][0]}`.toUpperCase();
}

const BackIcon = () => (
  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M15 18l-6-6 6-6" />
  </svg>
);

type TranscriptRowProps = {
  row: SegmentEntry;
  canManageSpeakers: boolean;
  isOpen: boolean;
  isAudioActive: boolean;
  isAudioPlaying: boolean;
  audioSeekEnabled: boolean;
  onOpen: (rowId: number) => void;
  onSeekToTime: (timeSec: number) => void;
  registerRowElement: (rowId: number, node: HTMLElement | null) => void;
};

function TranscriptRow({
  row,
  canManageSpeakers,
  isOpen,
  isAudioActive,
  isAudioPlaying,
  audioSeekEnabled,
  onOpen,
  onSeekToTime,
  registerRowElement,
}: TranscriptRowProps) {
  const isClickable = canManageSpeakers;
  const seekTimeSec = audioSeekEnabled ? row.start_sec ?? row.end_sec ?? null : null;
  const classNames = [
    "nt-stream-item",
    isClickable ? "is-clickable" : "",
    isOpen ? "is-open" : "",
    isAudioActive ? "is-audio-active" : "",
    row.needs_speaker_review ? "is-review-pending" : "",
  ]
    .filter(Boolean)
    .join(" ");

  const rowContent = (
    <div className="nt-stream-row-layout">
      <div className="nt-timeline-col">
        <span className={`nt-avatar nt-speaker-badge nt-avatar-${row.color}`}>{row.initials}</span>
        <span className="nt-timeline-line" />
      </div>
      <div className="nt-stream-content">
        <div className="nt-stream-meta-row">
          <div className="nt-stream-meta-stack">
            <strong className="nt-stream-speaker">{row.speaker}</strong>
            <span className="nt-stream-time">{row.timestamp}</span>
          </div>
          <div className="nt-transcript-pills">
            {isAudioPlaying ? <span className="nt-stream-live-indicator">Şu an oynatılıyor</span> : null}
            <StatusPill tone={toneForBindingState(row.needs_speaker_review ? "provisional" : "confirmed")}>
              {row.needs_speaker_review ? "Speaker review" : row.assignment_method}
            </StatusPill>
            <StatusPill tone="teal">{`%${Math.round(row.assignment_confidence * 100)}`}</StatusPill>
            {row.overlap_group_id ? <StatusPill tone="warning">Overlap</StatusPill> : null}
          </div>
        </div>
        <div
          className={[
            "nt-stream-bubble",
            seekTimeSec !== null ? "is-seekable" : "",
            row.needs_speaker_review ? "is-review-pending" : "",
          ]
            .filter(Boolean)
            .join(" ")}
          onClick={(event) => {
            if (seekTimeSec === null) {
              return;
            }
            event.preventDefault();
            event.stopPropagation();
            onSeekToTime(Math.max(0, seekTimeSec));
          }}
        >
          <p className="nt-stream-text">{row.text}</p>
        </div>
      </div>
    </div>
  );

  return (
    <article className={classNames} ref={(node) => registerRowElement(row.id, node)}>
      {isClickable ? (
        <button className="nt-stream-item-trigger" onClick={() => onOpen(row.id)} type="button">
          {rowContent}
        </button>
      ) : (
        rowContent
      )}
    </article>
  );
}

function findActiveSegmentId(rows: SegmentEntry[], currentTime: number) {
  const timedRows = rows
    .filter((row) => row.start_sec !== null || row.end_sec !== null)
    .sort((left, right) => {
      const leftStart = left.start_sec ?? left.end_sec ?? Number.POSITIVE_INFINITY;
      const rightStart = right.start_sec ?? right.end_sec ?? Number.POSITIVE_INFINITY;
      if (leftStart !== rightStart) {
        return leftStart - rightStart;
      }
      return left.id - right.id;
    });

  for (let index = 0; index < timedRows.length; index += 1) {
    const row = timedRows[index];
    const nextRow = timedRows[index + 1] ?? null;
    const start = row.start_sec ?? 0;
    const fallbackEnd = nextRow?.start_sec ?? start + 4;
    const end = row.end_sec !== null && row.end_sec > start ? row.end_sec : fallbackEnd;

    if (currentTime >= Math.max(0, start - 0.15) && currentTime < end) {
      return row.id;
    }
  }

  return null;
}

function hasReliableTranscriptAudioSync(rows: SegmentEntry[], durationSec: number) {
  if (!Number.isFinite(durationSec) || durationSec <= 0) {
    return true;
  }

  const timedRows = rows.filter((row) => row.start_sec !== null || row.end_sec !== null);
  if (timedRows.length < 2) {
    return true;
  }

  const rowTimes = timedRows
    .map((row) => row.end_sec ?? row.start_sec ?? null)
    .filter((value): value is number => value !== null);
  if (rowTimes.length === 0) {
    return true;
  }

  const earliestStart = Math.min(
    ...timedRows.map((row) => row.start_sec ?? row.end_sec ?? Number.POSITIVE_INFINITY),
  );
  const latestEnd = Math.max(...rowTimes);
  const overflowCount = timedRows.filter((row) => {
    const rowEnd = row.end_sec ?? row.start_sec ?? 0;
    return rowEnd > durationSec + 1.5;
  }).length;

  if (earliestStart > durationSec + 1.5) {
    return false;
  }
  if (latestEnd > durationSec + 2.5) {
    return false;
  }
  if (overflowCount > Math.max(1, Math.floor(timedRows.length * 0.2))) {
    return false;
  }
  return true;
}

type SpeakerModalProps = {
  row: SegmentEntry;
  participants: ParticipantEntry[];
  busy: boolean;
  onAssign: (participantId: number | null) => void;
  onSplit: (displayName: string) => void;
  onClose: () => void;
};

function SpeakerModal({ row, participants, busy, onAssign, onSplit, onClose }: SpeakerModalProps) {
  const [selectedParticipantId, setSelectedParticipantId] = useState<string>(
    row.participant_id === null ? "" : String(row.participant_id),
  );
  const [splitName, setSplitName] = useState(row.speaker === "Unknown" ? "" : `${row.speaker} alt akisi`);

  useEffect(() => {
    setSelectedParticipantId(row.participant_id === null ? "" : String(row.participant_id));
    setSplitName(row.speaker === "Unknown" ? "" : `${row.speaker} alt akisi`);
  }, [row.id, row.participant_id, row.speaker]);

  const review = row.review;

  return (
    <div className="nt-review-modal-shell" onClick={onClose} role="presentation">
      <div
        className="nt-review-modal nt-inline-review-popover"
        onClick={(event) => event.stopPropagation()}
        role="dialog"
        aria-modal="true"
        aria-labelledby={`speaker-modal-title-${row.id}`}
      >
        <div className="nt-review-head">
          <div>
            <strong id={`speaker-modal-title-${row.id}`}>{row.speaker}</strong>
            <span>{row.timestamp}</span>
          </div>
          <div className="nt-review-head-actions">
            <div className="nt-transcript-pills">
              <StatusPill tone="primary">{row.assignment_method}</StatusPill>
              <StatusPill tone="teal">{`%${Math.round(row.assignment_confidence * 100)}`}</StatusPill>
            </div>
            <button className="nt-btn nt-btn-ghost nt-btn-sm" onClick={onClose} type="button">
              Kapat
            </button>
          </div>
        </div>

        <div className="nt-review-block">
          <span>Transcript</span>
          <p>{row.text}</p>
        </div>

        {review ? (
          <>
            <div className="nt-review-block">
              <span>Mevcut speaker sinyali</span>
              <p>{review.current_text || "Mevcut suggestion yok."}</p>
            </div>
            <div className="nt-review-block">
              <span>Önerilen düzeltme</span>
              <p>{review.suggested_text || "Yeni suggestion yok."}</p>
            </div>
          </>
        ) : null}

        <div className="nt-review-block">
          <span>Speaker ataması</span>
          <select
            className="nt-input"
            disabled={busy}
            value={selectedParticipantId}
            onChange={(event) => setSelectedParticipantId(event.target.value)}
          >
            <option value="">Unknown olarak bırak</option>
            {participants.map((participant) => (
              <option key={participant.id} value={participant.id}>
                {participant.display_name}
              </option>
            ))}
          </select>
        </div>

        {review?.has_audio_clip && review.audio_clip_url ? (
          <div className="nt-review-block">
            <span>Ses klibi</span>
            <div className="nt-audio-section nt-audio-section-compact">
              <AudioPlayer compact preload="none" src={buildApiUrl(review.audio_clip_url)} />
            </div>
          </div>
        ) : null}

        {row.participant_id !== null ? (
          <div className="nt-review-block">
            <span>Split participant</span>
            <input
              className="nt-input"
              disabled={busy}
              placeholder="Yeni participant adı"
              value={splitName}
              onChange={(event) => setSplitName(event.target.value)}
            />
          </div>
        ) : null}

        <div className="nt-inline-actions">
          <button
            className="nt-btn nt-btn-primary"
            disabled={busy}
            onClick={() => onAssign(selectedParticipantId ? Number(selectedParticipantId) : null)}
            type="button"
          >
            {busy ? "Kaydediliyor" : "Atamayı kaydet"}
          </button>
          {row.participant_id !== null ? (
            <button
              className="nt-btn nt-btn-secondary"
              disabled={busy || splitName.trim().length < 2}
              onClick={() => onSplit(splitName.trim())}
              type="button"
            >
              Yeni participant olarak ayır
            </button>
          ) : null}
        </div>
      </div>
    </div>
  );
}

export function TranscriptPage() {
  const params = useParams();
  const meetingId = Number(params.meetingId);
  const [openRowId, setOpenRowId] = useState<number | null>(null);
  const [activeAudioRowId, setActiveAudioRowId] = useState<number | null>(null);
  const [isAudioPlaying, setIsAudioPlaying] = useState(false);
  const [audioDurationSec, setAudioDurationSec] = useState(0);
  const [stopRequested, setStopRequested] = useState(false);
  const [mergeSourceId, setMergeSourceId] = useState<string>("");
  const [mergeTargetId, setMergeTargetId] = useState<string>("");
  const audioRef = useRef<HTMLAudioElement | null>(null);
  const streamBodyRef = useRef<HTMLDivElement | null>(null);
  const transcriptRowElementsRef = useRef(new Map<number, HTMLElement>());

  const snapshot = useMeetingSnapshot(meetingId);
  const updateSegmentParticipant = useUpdateSegmentParticipant(meetingId);
  const mergeParticipants = useMergeParticipants(meetingId);
  const splitParticipant = useSplitParticipant(meetingId);
  const stopMeeting = useStopTranscriptMeeting(meetingId);
  const snapshotData = snapshot.data ?? null;
  const participants = snapshotData?.participants ?? [];
  const segments = snapshotData?.segments ?? [];
  const activeRow =
    openRowId === null || !snapshotData ? null : snapshotData.segments.find((row) => row.id === openRowId) ?? null;

  useEffect(() => {
    if (openRowId !== null && !activeRow) {
      setOpenRowId(null);
    }
  }, [activeRow, openRowId]);

  useEffect(() => {
    const availableParticipants = participants.filter((participant) => participant.join_state !== "merged");
    const sourceExists = availableParticipants.some((participant) => String(participant.id) === mergeSourceId);
    const targetExists = availableParticipants.some((participant) => String(participant.id) === mergeTargetId);
    if (!sourceExists) {
      setMergeSourceId(availableParticipants[0] ? String(availableParticipants[0].id) : "");
    }
    if (!targetExists) {
      setMergeTargetId(availableParticipants[1] ? String(availableParticipants[1].id) : "");
    }
  }, [mergeSourceId, mergeTargetId, participants]);

  useEffect(() => {
    if (openRowId === null) {
      return undefined;
    }
    const previousOverflow = document.body.style.overflow;
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setOpenRowId(null);
      }
    };
    document.body.style.overflow = "hidden";
    window.addEventListener("keydown", handleKeyDown);
    return () => {
      document.body.style.overflow = previousOverflow;
      window.removeEventListener("keydown", handleKeyDown);
    };
  }, [openRowId]);

  useEffect(() => {
    setAudioDurationSec(0);
  }, [snapshotData?.audio.audio_url]);

  useEffect(() => {
    const audioElement = audioRef.current;
    if (!audioElement || !snapshotData) {
      setActiveAudioRowId(null);
      setIsAudioPlaying(false);
      return;
    }
    if (!hasReliableTranscriptAudioSync(snapshotData.segments, audioDurationSec || audioElement.duration)) {
      setActiveAudioRowId(null);
      setIsAudioPlaying(!audioElement.paused && !audioElement.ended);
      return;
    }
    const nextActiveRowId = findActiveSegmentId(snapshotData.segments, audioElement.currentTime);
    setActiveAudioRowId((currentValue) => (currentValue === nextActiveRowId ? currentValue : nextActiveRowId));
    setIsAudioPlaying(!audioElement.paused && !audioElement.ended);
  }, [audioDurationSec, snapshotData]);

  useEffect(() => {
    if (activeAudioRowId === null) {
      return;
    }

    const container = streamBodyRef.current;
    const rowElement = transcriptRowElementsRef.current.get(activeAudioRowId);
    if (!container || !rowElement) {
      return;
    }

    const containerRect = container.getBoundingClientRect();
    const rowRect = rowElement.getBoundingClientRect();
    const topThreshold = containerRect.top + 96;
    const bottomThreshold = containerRect.bottom - 64;
    if (rowRect.top >= topThreshold && rowRect.bottom <= bottomThreshold) {
      return;
    }

    rowElement.scrollIntoView({
      behavior: "smooth",
      block: "center",
    });
  }, [activeAudioRowId]);

  useEffect(() => {
    if (stopMeeting.isError || !snapshotData?.actions.can_stop_meeting) {
      setStopRequested(false);
    }
  }, [snapshotData?.actions.can_stop_meeting, stopMeeting.isError]);

  if (!Number.isFinite(meetingId)) {
    return <LoadingView label="Geçersiz meeting kimliği" />;
  }

  if (snapshot.isLoading) {
    return <LoadingView label="Transcript snapshot yükleniyor" />;
  }

  if (snapshot.error || !snapshot.data) {
    return (
      <AppShell
        title="Transcript"
        subtitle="Snapshot yüklenemedi."
        aboveTitle={
          <Link className="nt-page-backlink" to="/dashboard">
            <BackIcon />
            <span>Toplantılar</span>
          </Link>
        }
      >
        <div className="nt-alert">{snapshot.error?.message ?? "Bilinmeyen hata"}</div>
      </AppShell>
    );
  }

  const data = snapshot.data;
  const progressActive =
    ["binding_sources", "materializing_audio", "transcribing_participants", "assembling_segments"].includes(
      data.postprocess.status,
    ) && data.postprocess.progress_pct !== null;
  const stopButtonBusy = stopRequested || stopMeeting.isPending;
  const isTranscriptAudioSyncReliable = hasReliableTranscriptAudioSync(data.segments, audioDurationSec);
  const effectiveActiveAudioRowId = isTranscriptAudioSyncReliable ? activeAudioRowId : null;
  const audioSyncNotice =
    audioDurationSec > 0 && !isTranscriptAudioSyncReliable
      ? "Bu kayıtta ses süresi transcript zamanlarıyla örtüşmüyor. Satır vurgusu ve satırdan oynatma kapatıldı."
      : null;
  const mergeBusy = mergeParticipants.isPending;
  const manageableParticipants = participants.filter((participant) => participant.join_state !== "merged");

  function syncAudioTranscript(currentTime: number) {
    if (!isTranscriptAudioSyncReliable) {
      setActiveAudioRowId(null);
      return;
    }
    const nextActiveRowId = findActiveSegmentId(data.segments, currentTime);
    setActiveAudioRowId((currentValue) => (currentValue === nextActiveRowId ? currentValue : nextActiveRowId));
  }

  function registerTranscriptRowElement(rowId: number, node: HTMLElement | null) {
    if (node) {
      transcriptRowElementsRef.current.set(rowId, node);
      return;
    }
    transcriptRowElementsRef.current.delete(rowId);
  }

  function seekAudioToTime(timeSec: number) {
    if (!isTranscriptAudioSyncReliable) {
      return;
    }

    const audioElement = audioRef.current;
    if (!audioElement) {
      return;
    }

    const applySeek = () => {
      audioElement.currentTime = Math.max(0, timeSec);
      syncAudioTranscript(audioElement.currentTime);
      void audioElement.play().catch(() => {
        setIsAudioPlaying(false);
      });
    };

    if (audioElement.readyState >= 1) {
      applySeek();
      return;
    }

    const handleLoadedMetadata = () => {
      applySeek();
      audioElement.removeEventListener("loadedmetadata", handleLoadedMetadata);
    };
    audioElement.addEventListener("loadedmetadata", handleLoadedMetadata);
    audioElement.load();
  }

  function requestStopMeeting() {
    if (stopButtonBusy) {
      return;
    }

    setStopRequested(true);
    void stopMeeting.mutateAsync().catch(() => {
      setStopRequested(false);
    });
  }

  function requestMergeParticipants() {
    const sourceParticipantId = Number(mergeSourceId);
    const targetParticipantId = Number(mergeTargetId);
    if (!sourceParticipantId || !targetParticipantId || sourceParticipantId === targetParticipantId) {
      return;
    }
    void mergeParticipants
      .mutateAsync({ sourceParticipantId, targetParticipantId })
      .then(() => {
        setOpenRowId(null);
      })
      .catch(() => undefined);
  }

  return (
    <AppShell
      title={data.meeting.title}
      subtitle="Transcript'i participant registry, audio binding ve speaker review akışıyla yönet."
      aboveTitle={
        <Link className="nt-page-backlink" to="/dashboard">
          <BackIcon />
          <span>Toplantılar</span>
        </Link>
      }
      titleAction={
        data.actions.can_stop_meeting ? (
          <button
            aria-busy={stopButtonBusy}
            className={`nt-btn nt-btn-danger nt-btn-sm nt-transcript-stop-btn ${stopButtonBusy ? "is-busy" : ""}`}
            disabled={stopButtonBusy}
            onClick={requestStopMeeting}
            type="button"
          >
            {stopButtonBusy ? (
              <>
                <span aria-hidden="true" className="nt-transcript-stop-dot" />
                Durdurma isteniyor
              </>
            ) : (
              "Toplantıyı durdur"
            )}
          </button>
        ) : null
      }
    >
      <section className="nt-transcript-top-layout">
        <div className="nt-top-main-stack">
          <article className="nt-card nt-card-padded nt-transcript-summary-card">
            <div className="nt-transcript-summary-row">
              <div className="nt-transcript-summary-item">
                <span className="nt-card-label">Meeting</span>
                <div className="nt-transcript-summary-value">
                  <StatusPill tone={toneForStatus(data.meeting.status)}>{data.meeting.status}</StatusPill>
                </div>
                <p className="nt-card-hint">{`${data.summary.speaker_count} participant · ${data.summary.segment_count} segment`}</p>
              </div>

              <div className="nt-transcript-summary-item">
                <span className="nt-card-label">Postprocess</span>
                <div className="nt-transcript-summary-value">
                  {progressActive ? (
                    <strong className="nt-transcript-summary-number">%{data.postprocess.progress_pct}</strong>
                  ) : (
                    <StatusPill tone={toneForStatus(data.postprocess.status)}>{data.postprocess.status}</StatusPill>
                  )}
                </div>
                <p className="nt-card-hint">{data.postprocess.progress_note ?? data.postprocess.error ?? "Worker idle"}</p>
              </div>

              <div className="nt-transcript-summary-item">
                <span className="nt-card-label">Speaker review</span>
                <div className="nt-transcript-summary-value">
                  <strong className="nt-transcript-summary-number">{data.actions.pending_review_count}</strong>
                </div>
                <p className="nt-card-hint">{`${participants.length} registry kaydı`}</p>
              </div>
            </div>

            <details className="nt-preview-disclosure nt-preview-disclosure-inline">
              <summary className="nt-preview-disclosure-summary">
                <div>
                  <p className="nt-card-label">Canlı önizleme</p>
                  <strong className="nt-preview-disclosure-title">
                    {data.preview.label || "Son görüntü"}
                  </strong>
                </div>
              </summary>

              <div className="nt-preview-disclosure-body">
                {data.preview.has_preview && data.preview.image_url ? (
                  <div className="nt-preview-frame">
                    <img
                      alt="Canlı meeting önizlemesi"
                      className="nt-preview-image"
                      src={buildApiUrl(data.preview.image_url)}
                    />
                  </div>
                ) : (
                  <div className="nt-preview-empty">
                    <strong>Henüz önizleme yok</strong>
                    <span>{data.preview.label || "Canlı önizleme toplantı sırasında görünür."}</span>
                  </div>
                )}
              </div>
            </details>
          </article>

          <article className="nt-card nt-card-padded nt-audio-card">
            <div className="nt-card-head">
              <div>
                <p className="nt-card-label">Ses kaydı</p>
                {!data.audio.has_audio ? <h2 className="nt-section-title">{data.audio.label}</h2> : null}
              </div>
            </div>
            {data.audio.has_audio && data.audio.audio_url ? (
              <div className="nt-audio-section nt-audio-section-plain">
                <AudioPlayer
                  className="nt-audio-player-flat"
                  compact
                  ref={audioRef}
                  preload="metadata"
                  src={buildApiUrl(data.audio.audio_url)}
                  onLoadedMetadata={(event) => {
                    setAudioDurationSec(
                      Number.isFinite(event.currentTarget.duration) && event.currentTarget.duration > 0
                        ? event.currentTarget.duration
                        : 0,
                    );
                    syncAudioTranscript(event.currentTarget.currentTime);
                  }}
                  onDurationChange={(event) => {
                    setAudioDurationSec(
                      Number.isFinite(event.currentTarget.duration) && event.currentTarget.duration > 0
                        ? event.currentTarget.duration
                        : 0,
                    );
                  }}
                  onPlay={(event) => {
                    setIsAudioPlaying(true);
                    syncAudioTranscript(event.currentTarget.currentTime);
                  }}
                  onPause={() => setIsAudioPlaying(false)}
                  onEnded={(event) => {
                    setIsAudioPlaying(false);
                    syncAudioTranscript(event.currentTarget.currentTime);
                  }}
                  onSeeked={(event) => syncAudioTranscript(event.currentTarget.currentTime)}
                  onTimeUpdate={(event) => syncAudioTranscript(event.currentTarget.currentTime)}
                />
                {audioSyncNotice ? <p className="nt-audio-sync-note">{audioSyncNotice}</p> : null}
              </div>
            ) : (
              <div className="nt-empty-state">
                <strong>Ses kaydı hazır değil</strong>
                <span>{data.audio.error ?? "Bu meeting için oynatılabilir audio yok."}</span>
              </div>
            )}
          </article>
        </div>

        <aside className="nt-top-side-stack">
          <article className="nt-card nt-card-padded nt-participants-card">
            <div className="nt-card-head">
              <div>
                <p className="nt-card-label">Katılımcılar</p>
                <h2 className="nt-section-title">Konuşmacılar</h2>
                <p className="nt-card-hint">Toplantıda gördüğümüz kişiler burada listelenir.</p>
              </div>
            </div>
            <div className="nt-participants-card-body">
              {manageableParticipants.length === 0 ? (
                <div className="nt-empty-state">
                  <strong>Henüz konuşmacı görünmüyor</strong>
                  <span>Toplantı başladıktan sonra kişiler burada belirecek.</span>
                </div>
              ) : (
                <div className="nt-stream-track">
                  {manageableParticipants.map((participant) => (
                    <div className="nt-stream-item" key={participant.id}>
                      <div className="nt-stream-row-layout">
                        <div className="nt-timeline-col">
                          <span className="nt-avatar nt-speaker-badge nt-avatar-blue">
                            {initialsForParticipant(participant.display_name)}
                          </span>
                        </div>
                        <div className="nt-stream-content">
                          <div className="nt-stream-meta-row">
                            <div className="nt-stream-meta-stack">
                              <strong className="nt-stream-speaker">{participant.display_name}</strong>
                              <span className="nt-stream-time">{helperTextForParticipant(participant)}</span>
                            </div>
                            <div className="nt-transcript-pills">
                              <StatusPill tone={toneForBindingState(participant.binding_state)}>
                                {labelForBindingState(participant.binding_state)}
                              </StatusPill>
                            </div>
                          </div>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
            {data.actions.can_manage_speakers && manageableParticipants.length > 1 ? (
              <details className="nt-transcript-advanced-tools nt-transcript-advanced-tools-side">
                <summary className="nt-transcript-advanced-summary">Konuşmacı listesinde bir hata mı var?</summary>
                <p className="nt-card-hint">Aynı kişi iki kez görünüyorsa aşağıdan birleştirebilirsin.</p>
                <div className="nt-merge-form">
                  <div className="nt-merge-grid">
                    <label className="nt-merge-field">
                      <span className="nt-merge-field-label">Birleştir</span>
                      <span className="nt-merge-select-shell">
                        <select className="nt-input nt-merge-select" value={mergeSourceId} onChange={(event) => setMergeSourceId(event.target.value)}>
                          {manageableParticipants.map((participant) => (
                            <option key={`source-${participant.id}`} value={participant.id}>
                              {participant.display_name}
                            </option>
                          ))}
                        </select>
                      </span>
                    </label>
                    <label className="nt-merge-field">
                      <span className="nt-merge-field-label">Buna</span>
                      <span className="nt-merge-select-shell">
                        <select className="nt-input nt-merge-select" value={mergeTargetId} onChange={(event) => setMergeTargetId(event.target.value)}>
                          {manageableParticipants.map((participant) => (
                            <option key={`target-${participant.id}`} value={participant.id}>
                              {participant.display_name}
                            </option>
                          ))}
                        </select>
                      </span>
                    </label>
                  </div>
                  <button
                    className="nt-btn nt-btn-secondary nt-btn-sm nt-merge-submit"
                    disabled={mergeBusy || !mergeSourceId || !mergeTargetId || mergeSourceId === mergeTargetId}
                    onClick={requestMergeParticipants}
                    type="button"
                  >
                    {mergeBusy ? "Birleştiriliyor" : "İki kişiyi birleştir"}
                  </button>
                </div>
              </details>
            ) : null}
          </article>
        </aside>
      </section>

      <section className="nt-stream-shell nt-transcript-panel">
        <div className="nt-stream-header">
          <div>
            <p className="nt-card-label">Transcript</p>
            <h2 className="nt-section-title">Segment listesi</h2>
            <p className="nt-review-helper is-muted">
              Satıra tıklayarak speaker ataması yapabilir, gerektiğinde Unknown bırakabilir veya tek satırı yeni participant'a ayırabilirsiniz.
            </p>
          </div>
          <div className="nt-stream-actions">
            <a className="nt-btn nt-btn-primary nt-btn-sm" href={buildApiUrl(`/api/meetings/${meetingId}/export.txt`)}>
              TXT indir
            </a>
            <a className="nt-btn nt-btn-primary nt-btn-sm" href={buildApiUrl(`/api/meetings/${meetingId}/export.csv`)}>
              CSV indir
            </a>
          </div>
        </div>
        {updateSegmentParticipant.error ? <div className="nt-alert">{updateSegmentParticipant.error.message}</div> : null}
        {mergeParticipants.error ? <div className="nt-alert">{mergeParticipants.error.message}</div> : null}
        {splitParticipant.error ? <div className="nt-alert">{splitParticipant.error.message}</div> : null}
        <div className="nt-stream-body" ref={streamBodyRef}>
          <div className="nt-stream-track nt-transcript-list">
            {segments.length === 0 ? (
              <div className="nt-empty-state">
                <strong>Henüz transcript segmenti yok</strong>
                <span>Audio-first postprocess tamamlandığında participant bazlı transcript burada görünecek.</span>
              </div>
            ) : null}
            {segments.map((row) => (
              <TranscriptRow
                key={row.id}
                audioSeekEnabled={isTranscriptAudioSyncReliable}
                canManageSpeakers={data.actions.can_manage_speakers}
                isAudioActive={row.id === effectiveActiveAudioRowId}
                isAudioPlaying={isAudioPlaying && row.id === effectiveActiveAudioRowId}
                isOpen={row.id === openRowId}
                onOpen={setOpenRowId}
                onSeekToTime={seekAudioToTime}
                registerRowElement={registerTranscriptRowElement}
                row={row}
              />
            ))}
          </div>
        </div>
      </section>

      {activeRow ? (
        <SpeakerModal
          busy={updateSegmentParticipant.isPending || splitParticipant.isPending}
          onAssign={(participantId) =>
            void updateSegmentParticipant
              .mutateAsync({ segmentId: activeRow.id, participantId })
              .then(() => setOpenRowId(null))
              .catch(() => undefined)
          }
          onClose={() => setOpenRowId(null)}
          onSplit={(displayName) =>
            void splitParticipant
              .mutateAsync({
                participantId: activeRow.participant_id ?? 0,
                segmentIds: [activeRow.id],
                displayName,
              })
              .then(() => setOpenRowId(null))
              .catch(() => undefined)
          }
          participants={manageableParticipants}
          row={activeRow}
        />
      ) : null}
    </AppShell>
  );
}
