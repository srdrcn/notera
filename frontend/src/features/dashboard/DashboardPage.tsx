import { zodResolver } from "@hookform/resolvers/zod";
import { useForm } from "react-hook-form";
import { useNavigate } from "react-router-dom";
import { z } from "zod";

import { AppShell } from "../../components/AppShell";
import { MetricCard } from "../../components/MetricCard";
import { StatusPill } from "../../components/StatusPill";
import { useSession } from "../../app/session";
import {
  useCreateMeeting,
  useDeleteMeeting,
  useJoinMeeting,
  useMeetings,
  useStopMeeting,
} from "./useMeetings";


const TEAMS_URL_PATTERN = /https?:\/\/[^\s<>"']+/gi;


function cleanUrlCandidate(candidate: string) {
  return candidate.trim().replace(/^<|>$/g, "").replace(/[).,;!?]+$/g, "");
}


function isSupportedTeamsUrl(candidate: string) {
  const parsed = new URL(candidate);
  const hostname = parsed.hostname.toLowerCase();
  const path = parsed.pathname.toLowerCase();

  if (hostname === "teams.live.com" || hostname.endsWith(".teams.live.com")) {
    return true;
  }
  if (hostname === "teams.microsoft.com" || hostname.endsWith(".teams.microsoft.com")) {
    return true;
  }
  if (hostname.endsWith("microsoft.com") && path.includes("/microsoft-teams/join-a-meeting")) {
    return true;
  }
  return false;
}


function extractTeamsJoinTarget(rawValue: string) {
  const value = rawValue.trim();
  if (!value) {
    return null;
  }

  const urlMatches = value.match(TEAMS_URL_PATTERN) ?? [];
  for (const match of urlMatches) {
    const candidate = cleanUrlCandidate(match);
    try {
      if (isSupportedTeamsUrl(candidate)) {
        return candidate;
      }
    } catch {
      // Ignore malformed candidates and continue.
    }
  }

  return null;
}


function extractTeamsJoinTargetFromHtml(rawHtml: string) {
  if (!rawHtml || typeof DOMParser === "undefined") {
    return null;
  }

  try {
    const parser = new DOMParser();
    const doc = parser.parseFromString(rawHtml, "text/html");
    const anchors = Array.from(doc.querySelectorAll("a[href]"));

    for (const anchor of anchors) {
      const href = anchor.getAttribute("href");
      if (!href) {
        continue;
      }
      const teamsLink = extractTeamsJoinTarget(href);
      if (teamsLink) {
        return teamsLink;
      }
    }
  } catch {
    return null;
  }

  return null;
}


const meetingSchema = z.object({
  title: z.string().trim().min(2, "Toplantı adı en az 2 karakter olmalı."),
  teams_link: z
    .string()
    .trim()
    .min(1, "Teams toplantı linki girin.")
    .refine((value) => extractTeamsJoinTarget(value) !== null, "Geçerli bir Teams toplantı linki girin."),
  audio_recording_enabled: z.boolean(),
});

type MeetingForm = z.infer<typeof meetingSchema>;


function toneForStatus(status: string): "default" | "success" | "warning" | "danger" | "teal" | "primary" {
  if (status === "completed" || status === "review_ready") {
    return "success";
  }
  if (status === "active" || status === "joining") {
    return "primary";
  }
  if (["binding_sources", "transcribing_participants", "assembling_segments", "transcribing", "aligning"].includes(status)) {
    return "teal";
  }
  if (status === "failed") {
    return "danger";
  }
  if (["queued", "pending", "materializing_audio", "canonicalizing", "rebuilding"].includes(status)) {
    return "warning";
  }
  return "default";
}


function formatDate(value: string | null) {
  if (!value) {
    return "Henüz başlamadı";
  }
  return new Date(value).toLocaleString("tr-TR");
}


const TrashIcon = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <polyline points="3 6 5 6 21 6"></polyline>
    <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"></path>
  </svg>
);

export function DashboardPage() {
  const navigate = useNavigate();
  const session = useSession();
  const meetings = useMeetings();
  const createMeeting = useCreateMeeting();
  const joinMeeting = useJoinMeeting();
  const stopMeeting = useStopMeeting();
  const deleteMeeting = useDeleteMeeting();
  const form = useForm<MeetingForm>({
    resolver: zodResolver(meetingSchema),
    defaultValues: {
      title: "",
      teams_link: "",
      audio_recording_enabled: true,
    },
  });
  const teamsLinkField = form.register("teams_link");

  function syncTeamsLinkField(rawValue: string) {
    const teamsLink = extractTeamsJoinTarget(rawValue);
    if (!teamsLink || teamsLink === rawValue) {
      return;
    }
    form.setValue("teams_link", teamsLink, {
      shouldDirty: true,
      shouldTouch: true,
      shouldValidate: true,
    });
  }

  async function onSubmit(values: MeetingForm) {
    const teamsLink = extractTeamsJoinTarget(values.teams_link);
    if (!teamsLink) {
      form.setError("teams_link", {
        type: "validate",
        message: "Geçerli bir Teams toplantı linki girin.",
      });
      return;
    }

    try {
      await createMeeting.mutateAsync({
        ...values,
        teams_link: teamsLink,
      });
      form.reset({
        title: "",
        teams_link: "",
        audio_recording_enabled: true,
      });
    } catch {
      // User-facing error is already exposed via mutation state.
    }
  }

  const rows = meetings.data ?? [];
  const activeCount = rows.filter((row) => row.status === "joining" || row.status === "active").length;

  function openTranscript(meetingId: number) {
    navigate(`/transcripts/${meetingId}`);
  }

  return (
    <AppShell
      title="Dashboard"
      subtitle="Toplantılarını başlat, audio-first capture durumunu takip et ve transcript'leri tek yerden yönet."
      navSlot={
        <button className="nt-btn nt-btn-secondary nt-btn-sm" onClick={() => void session.logout()} type="button">
          Çıkış yap
        </button>
      }
    >
      <section className="nt-dashboard-stack">
        <article className="nt-card nt-card-padded nt-dashboard-composer">
          <div className="nt-card-head nt-dashboard-composer-head">
            <div>
              <p className="nt-card-label">Yeni toplantı</p>
              <h2 className="nt-section-title">Toplantı başlat</h2>
              <p className="nt-card-hint">Toplantı adını ve gerçek Teams linkini gir, Notera botu participant registry ve audio capture akışını başlatsın.</p>
            </div>
          </div>
          <form className="nt-dashboard-composer-form" onSubmit={form.handleSubmit(onSubmit)}>
            <div className="nt-dashboard-composer-fields">
              <label className="nt-field nt-dashboard-field-title">
                <span>Toplantı adı</span>
                <input className="nt-input" placeholder="Ürün senkronu" {...form.register("title")} />
                {form.formState.errors.title ? <small>{form.formState.errors.title.message}</small> : null}
              </label>
              <label className="nt-field nt-dashboard-field-link">
                <span>Teams toplantı linki</span>
                <input
                  className="nt-input nt-input-mono"
                  placeholder="https://teams.live.com/meet/..."
                  {...teamsLinkField}
                  onChange={(event) => {
                    teamsLinkField.onChange(event);
                    syncTeamsLinkField(event.target.value);
                  }}
                  onPaste={(event) => {
                    const htmlValue = event.clipboardData.getData("text/html");
                    const teamsLinkFromHtml = extractTeamsJoinTargetFromHtml(htmlValue);
                    if (!teamsLinkFromHtml) {
                      return;
                    }

                    event.preventDefault();
                    form.setValue("teams_link", teamsLinkFromHtml, {
                      shouldDirty: true,
                      shouldTouch: true,
                      shouldValidate: true,
                    });
                  }}
                />
                {form.formState.errors.teams_link ? <small>{form.formState.errors.teams_link.message}</small> : null}
              </label>
            </div>
            <div className="nt-dashboard-composer-actions">
              <label className="nt-checkbox nt-dashboard-composer-toggle">
                <input type="checkbox" {...form.register("audio_recording_enabled")} />
                <span aria-hidden="true" className="nt-dashboard-toggle-ui" />
                <span className="nt-dashboard-toggle-copy">Toplantı sesi kaydedilsin</span>
              </label>
              <button className="nt-btn nt-btn-primary nt-dashboard-composer-submit" disabled={createMeeting.isPending} type="submit">
                {createMeeting.isPending ? "Katılıyor" : "Başlat"}
              </button>
            </div>
            {createMeeting.error ? (
              <div className="nt-alert">{createMeeting.error.message}</div>
            ) : null}
          </form>
        </article>

        <section className="nt-metric-grid">
          <MetricCard className="is-compact" accent="primary" label="Toplantılar" value={rows.length} />
          <MetricCard className="is-compact" accent="teal" label="Aktif" value={activeCount} />
        </section>

        <article className="nt-card nt-card-padded">
          <div className="nt-card-head">
            <div>
              <p className="nt-card-label">Toplantılar</p>
              <h2 className="nt-section-title">Tüm kayıtlar</h2>
              <p className="nt-card-hint">Detaylar ve transcript için kartlardan birine tıkla.</p>
            </div>
          </div>
          {meetings.error ? (
            <div className="nt-alert">{meetings.error.message}</div>
          ) : null}
          <div className="nt-meeting-list nt-grid">
            {rows.length === 0 ? (
              <div className="nt-empty-state">
                <strong>Henüz meeting yok</strong>
                <span>İlk kaydı yukarıdaki alandan başlatabilirsin.</span>
              </div>
            ) : null}
            {rows.map((meeting) => (
              <article
                className="nt-card nt-meeting-card"
                key={meeting.id}
                onClick={() => openTranscript(meeting.id)}
                onKeyDown={(event) => {
                  if (event.target !== event.currentTarget) {
                    return;
                  }
                  if (event.key === "Enter" || event.key === " ") {
                    event.preventDefault();
                    openTranscript(meeting.id);
                  }
                }}
                role="link"
                tabIndex={0}
              >
                <div className="nt-meeting-head">
                  <div>
                    <h3 className="nt-meeting-title">{meeting.title}</h3>
                    <p className="nt-caption">{formatDate(meeting.created_at)}</p>
                  </div>
                  <div className="nt-meeting-pills">
                    <StatusPill tone={toneForStatus(meeting.status)}>{meeting.status}</StatusPill>
                  </div>
                </div>

                <div className="nt-meeting-mini-meta">
                   {meeting.audio_status !== "not_recorded" ? <span>● {meeting.audio_status}</span> : null}
                   <span>{meeting.status !== "active" && meeting.joined_at ? ` · ${formatDate(meeting.joined_at).split(" ")[1]}` : ""}</span>
                </div>

                {meeting.postprocess_progress_note ? (
                  <p className="nt-meeting-note">
                    {meeting.postprocess_progress_pct !== null ? `%${meeting.postprocess_progress_pct} · ` : ""}
                    {meeting.postprocess_progress_note}
                  </p>
                ) : null}
                <div className="nt-inline-actions nt-meeting-actions">
                  {meeting.can_join ? (
                    <button
                      className="nt-btn nt-btn-primary nt-btn-sm"
                      disabled={joinMeeting.isPending}
                      onClick={(event) => {
                        event.stopPropagation();
                        void joinMeeting.mutateAsync(meeting.id);
                      }}
                      type="button"
                    >
                      Katıl
                    </button>
                  ) : null}
                  {meeting.can_stop ? (
                    <button
                      className="nt-btn nt-btn-secondary nt-btn-sm"
                      disabled={stopMeeting.isPending}
                      onClick={(event) => {
                        event.stopPropagation();
                        void stopMeeting.mutateAsync(meeting.id);
                      }}
                      type="button"
                    >
                      Durdur
                    </button>
                  ) : null}
                  <button
                    aria-label="Toplantiyi sil"
                    className="nt-btn nt-btn-ghost nt-btn-sm nt-delete-btn"
                    disabled={deleteMeeting.isPending || meeting.can_stop}
                    onClick={(event) => {
                      event.stopPropagation();
                      void deleteMeeting.mutateAsync(meeting.id);
                    }}
                    title="Sil"
                    type="button"
                  >
                    <TrashIcon />
                  </button>
                </div>
              </article>
            ))}
          </div>
        </article>
      </section>
    </AppShell>
  );
}
