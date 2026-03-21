import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";

import { AppShell } from "../../components/AppShell";
import { LoadingView } from "../../components/LoadingView";
import { StatusPill } from "../../components/StatusPill";
import { buildApiUrl } from "../../lib/api/client";
import type { TranscriptEntry } from "../../lib/api/types";
import {
  useApplyAllReviews,
  useApplyReview,
  useKeepReview,
  useMeetingSnapshot,
  useMergeDuplicates,
  useStopTranscriptMeeting,
} from "./useMeetingSnapshot";


function toneForStatus(status: string): "default" | "success" | "warning" | "danger" | "teal" {
  if (status === "completed" || status === "review_ready") {
    return "success";
  }
  if (status === "failed") {
    return "danger";
  }
  if (["joining", "active", "transcribing", "aligning"].includes(status)) {
    return "teal";
  }
  if (["queued", "pending", "canonicalizing", "rebuilding"].includes(status)) {
    return "warning";
  }
  return "default";
}


const BackIcon = () => (
  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M15 18l-6-6 6-6" />
  </svg>
);


type TranscriptRowProps = {
  row: TranscriptEntry;
  isOpen: boolean;
  onOpen: (rowId: number) => void;
};


function TranscriptRow({ row, isOpen, onOpen }: TranscriptRowProps) {
  const review = row.review;
  const isClickable = Boolean(review);

  const classNames = [
    "nt-stream-item",
    isClickable ? "is-clickable" : "",
    isOpen ? "is-open" : "",
    row.has_pending_review ? "is-review-pending" : "",
    row.has_duplicate_merge_candidate ? "is-merge-candidate" : "",
  ]
    .filter(Boolean)
    .join(" ");

  const openReview = () => {
    if (review) {
      onOpen(row.id);
    }
  };

  return (
    <article className={classNames}>
      {isClickable ? (
        <button
          className="nt-stream-item-trigger"
          onClick={openReview}
          onKeyDown={(event) => {
            if (event.key === "Enter" || event.key === " ") {
              event.preventDefault();
              openReview();
            }
          }}
          type="button"
        >
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
                  {row.auto_corrected ? <StatusPill tone="success">Auto corrected</StatusPill> : null}
                  {row.has_duplicate_merge_candidate ? (
                    <StatusPill tone="warning">Duplicate adayı</StatusPill>
                  ) : null}
                  {row.has_pending_review ? (
                    <StatusPill tone="primary">Review bekliyor</StatusPill>
                  ) : (
                    <StatusPill tone={row.resolution_status === "accepted" ? "success" : "default"}>
                      {row.resolution_status}
                    </StatusPill>
                  )}
                </div>
              </div>
              <div
                className={[
                  "nt-stream-bubble",
                  row.has_pending_review ? "is-review-pending" : "",
                  row.has_duplicate_merge_candidate ? "is-merge-candidate" : "",
                ]
                  .filter(Boolean)
                  .join(" ")}
              >
                <p className="nt-stream-text">{row.text}</p>
              </div>
            </div>
          </div>
        </button>
      ) : (
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
                {row.auto_corrected ? <StatusPill tone="success">Auto corrected</StatusPill> : null}
                {row.has_duplicate_merge_candidate ? (
                  <StatusPill tone="warning">Duplicate adayı</StatusPill>
                ) : null}
                <StatusPill tone={row.resolution_status === "accepted" ? "success" : "default"}>
                  {row.resolution_status}
                </StatusPill>
              </div>
            </div>
            <div
              className={[
                "nt-stream-bubble",
                row.has_duplicate_merge_candidate ? "is-merge-candidate" : "",
              ]
                .filter(Boolean)
                .join(" ")}
            >
              <p className="nt-stream-text">{row.text}</p>
            </div>
          </div>
        </div>
      )}
    </article>
  );
}


type TranscriptReviewModalProps = {
  row: TranscriptEntry;
  reviewBusy: boolean;
  onApply: (reviewId: number) => void;
  onKeep: (reviewId: number) => void;
  onClose: () => void;
};


function TranscriptReviewModal({
  row,
  reviewBusy,
  onApply,
  onKeep,
  onClose,
}: TranscriptReviewModalProps) {
  const review = row.review;

  if (!review) {
    return null;
  }

  return (
    <div
      className="nt-review-modal-shell"
      onClick={onClose}
      role="presentation"
    >
      <div
        className="nt-review-modal nt-inline-review-popover"
        onClick={(event) => event.stopPropagation()}
        role="dialog"
        aria-modal="true"
        aria-labelledby={`review-modal-title-${review.id}`}
      >
        <div className="nt-review-head">
          <div>
            <strong id={`review-modal-title-${review.id}`}>{row.speaker}</strong>
            <span>{row.timestamp}</span>
          </div>
            <div className="nt-review-head-actions">
              <div className="nt-transcript-pills">
                <StatusPill tone="primary">{review.granularity}</StatusPill>
                <StatusPill tone="teal">{review.confidence_label}</StatusPill>
              </div>
            <button className="nt-btn nt-btn-ghost nt-btn-sm" onClick={onClose} type="button">
              Kapat
            </button>
          </div>
        </div>
        <div className="nt-review-block">
          <span>Mevcut caption</span>
          <p>{review.current_text}</p>
        </div>
        <div className="nt-review-block">
          <span>WhisperX metni</span>
          <p>{review.suggested_text}</p>
        </div>
        <div className="nt-review-block">
          <span>Ses klibi</span>
          {review.has_audio_clip && review.audio_clip_url ? (
            <audio controls preload="none" src={buildApiUrl(review.audio_clip_url)} />
          ) : (
            <p>Bu öneri için ses klibi üretilemedi.</p>
          )}
        </div>
        <div className="nt-inline-actions">
          <button
            className="nt-btn nt-btn-primary"
            disabled={reviewBusy}
            onClick={() => onApply(review.id)}
            type="button"
          >
            Uygula
          </button>
          <button
            className="nt-btn nt-btn-secondary"
            disabled={reviewBusy}
            onClick={() => onKeep(review.id)}
            type="button"
          >
            Koru
          </button>
        </div>
      </div>
    </div>
  );
}


export function TranscriptPage() {
  const params = useParams();
  const meetingId = Number(params.meetingId);
  const [openReviewRowId, setOpenReviewRowId] = useState<number | null>(null);

  const snapshot = useMeetingSnapshot(meetingId);
  const applyReview = useApplyReview(meetingId);
  const keepReview = useKeepReview(meetingId);
  const applyAllReviews = useApplyAllReviews(meetingId);
  const mergeDuplicates = useMergeDuplicates(meetingId);
  const stopMeeting = useStopTranscriptMeeting(meetingId);
  const snapshotData = snapshot.data ?? null;
  const activeReviewRow =
    openReviewRowId === null || !snapshotData
      ? null
      : snapshotData.transcripts.find((row) => row.id === openReviewRowId && row.review) ?? null;

  useEffect(() => {
    if (openReviewRowId !== null && !activeReviewRow) {
      setOpenReviewRowId(null);
    }
  }, [activeReviewRow, openReviewRowId]);

  useEffect(() => {
    if (openReviewRowId === null) {
      return undefined;
    }
    const previousOverflow = document.body.style.overflow;
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setOpenReviewRowId(null);
      }
    };
    document.body.style.overflow = "hidden";
    window.addEventListener("keydown", handleKeyDown);
    return () => {
      document.body.style.overflow = previousOverflow;
      window.removeEventListener("keydown", handleKeyDown);
    };
  }, [openReviewRowId]);

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
        <div className="nt-alert">
          {snapshot.error?.message ?? "Bilinmeyen hata"}
        </div>
      </AppShell>
    );
  }

  const data = snapshot.data;

  const progressActive =
    ["transcribing", "aligning"].includes(data.postprocess.status) &&
    data.postprocess.progress_pct !== null;
  const hasSummaryActions =
    data.actions.can_apply_all_reviews || data.actions.can_merge_duplicate_transcripts;

  return (
    <AppShell
      title={data.meeting.title}
      subtitle="Final transcript, review akışı, canlı preview ve export işlemleri tek ekranda."
      aboveTitle={
        <Link className="nt-page-backlink" to="/dashboard">
          <BackIcon />
          <span>Toplantılar</span>
        </Link>
      }
      actions={
        <div className="nt-inline-actions">
          {data.actions.can_stop_meeting ? (
            <button
              className="nt-btn nt-btn-secondary nt-btn-sm"
              disabled={stopMeeting.isPending}
              onClick={() => void stopMeeting.mutateAsync()}
              type="button"
            >
              Toplantıyı durdur
            </button>
          ) : null}
        </div>
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
                <p className="nt-card-hint">{`${data.summary.speaker_count} konuşmacı · ${data.summary.transcript_count} satır`}</p>
              </div>

              <div className="nt-transcript-summary-item">
                <span className="nt-card-label">WhisperX</span>
                <div className="nt-transcript-summary-value">
                  {progressActive ? (
                    <strong className="nt-transcript-summary-number">%{data.postprocess.progress_pct}</strong>
                  ) : (
                    <StatusPill tone={toneForStatus(data.postprocess.status)}>{data.postprocess.status}</StatusPill>
                  )}
                </div>
                <p className="nt-card-hint">{data.postprocess.progress_note ?? data.postprocess.error ?? "Worker idle"}</p>
                {progressActive ? (
                  <div className="nt-progress-shell">
                    <div
                      className="nt-progress-bar"
                      style={{ width: `${data.postprocess.progress_pct ?? 0}%` }}
                    />
                  </div>
                ) : null}
              </div>

              <div className="nt-transcript-summary-item">
                <span className="nt-card-label">Review</span>
                <div className="nt-transcript-summary-value">
                  <strong className="nt-transcript-summary-number">{data.actions.pending_review_count}</strong>
                </div>
                <p className="nt-card-hint">{`${data.actions.duplicate_merge_candidate_count} duplicate aday`}</p>
              </div>
            </div>

            {hasSummaryActions ? (
              <div className="nt-inline-actions nt-transcript-summary-actions">
                {data.actions.can_apply_all_reviews ? (
                  <button
                    className="nt-btn nt-btn-primary nt-btn-sm"
                    disabled={applyAllReviews.isPending}
                    onClick={() => void applyAllReviews.mutateAsync()}
                    type="button"
                  >
                    {applyAllReviews.isPending ? "Uygulanıyor" : "Tümünü uygula"}
                  </button>
                ) : null}
                {data.actions.can_merge_duplicate_transcripts ? (
                  <button
                    className="nt-btn nt-btn-secondary nt-btn-sm"
                    disabled={mergeDuplicates.isPending}
                    onClick={() => void mergeDuplicates.mutateAsync()}
                    type="button"
                  >
                    {mergeDuplicates.isPending ? "Birleştiriliyor" : "Duplicate kayıtları birleştir"}
                  </button>
                ) : null}
              </div>
            ) : null}
          </article>

          <article className="nt-card nt-card-padded nt-audio-card">
            <div className="nt-card-head">
              <div>
                <p className="nt-card-label">Ses kaydı</p>
                <h2 className="nt-section-title">{data.audio.label}</h2>
              </div>
            </div>
            {data.audio.has_audio && data.audio.audio_url ? (
              <div className="nt-audio-section">
                <audio className="nt-audio-player" controls preload="metadata" src={buildApiUrl(data.audio.audio_url)} />
              </div>
            ) : (
              <div className="nt-empty-state">
                <strong>Ses kaydı hazır değil</strong>
                <span>{data.audio.error ?? "Bu meeting için oynatılabilir audio yok."}</span>
              </div>
            )}
          </article>
        </div>

        <div className="nt-top-preview-col">
          <article className="nt-card nt-card-padded nt-preview-card">
            <div className="nt-card-head">
              <div>
                <p className="nt-card-label">Canlı preview</p>
                <h2 className="nt-section-title">{data.preview.label}</h2>
              </div>
            </div>
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
                <strong>Preview yok</strong>
                <span>Bot henüz güncel kare üretmedi veya meeting aktif değil.</span>
              </div>
            )}
          </article>
        </div>
      </section>

      <section className="nt-stream-shell nt-transcript-panel">
        <div className="nt-stream-header">
          <div>
            <p className="nt-card-label">Transcript</p>
            <h2 className="nt-section-title">Inline review akışı</h2>
            <p className="nt-review-helper is-muted">
              Vurgulu satırlara tıklayarak review yapabilirsiniz. Duplicate adayları da aynı akışta işaretlenir.
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
        {applyReview.error ? (
          <div className="nt-alert">{applyReview.error.message}</div>
        ) : null}
        {keepReview.error ? (
          <div className="nt-alert">{keepReview.error.message}</div>
        ) : null}
        {mergeDuplicates.error ? (
          <div className="nt-alert">{mergeDuplicates.error.message}</div>
        ) : null}
        <div className="nt-stream-body">
          <div className="nt-stream-track nt-transcript-list">
          {data.transcripts.length === 0 ? (
            <div className="nt-empty-state">
              <strong>Henüz transcript yok</strong>
              <span>Toplantı aktifse canlı caption akışı birazdan burada görünür.</span>
            </div>
          ) : null}
          {data.transcripts.map((row) => (
            <TranscriptRow
              key={row.id}
              isOpen={row.id === openReviewRowId}
              onOpen={setOpenReviewRowId}
              row={row}
            />
          ))}
          </div>
        </div>
      </section>
      {activeReviewRow ? (
        <TranscriptReviewModal
          row={activeReviewRow}
          reviewBusy={applyReview.isPending || keepReview.isPending}
          onApply={(reviewId) => void applyReview.mutateAsync(reviewId).then(() => setOpenReviewRowId(null))}
          onClose={() => setOpenReviewRowId(null)}
          onKeep={(reviewId) => void keepReview.mutateAsync(reviewId).then(() => setOpenReviewRowId(null))}
        />
      ) : null}
    </AppShell>
  );
}
