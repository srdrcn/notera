import { zodResolver } from "@hookform/resolvers/zod";
import { useForm } from "react-hook-form";
import { Link } from "react-router-dom";
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


const meetingSchema = z.object({
  title: z.string().trim().min(2, "Toplantı adı en az 2 karakter olmalı."),
  teams_link: z.url("Geçerli bir Teams linki girin."),
  audio_recording_enabled: z.boolean(),
});

type MeetingForm = z.infer<typeof meetingSchema>;


function toneForStatus(status: string): "default" | "success" | "warning" | "danger" | "teal" {
  if (status === "completed" || status === "review_ready") {
    return "success";
  }
  if (status === "active" || status === "joining" || status === "transcribing" || status === "aligning") {
    return "teal";
  }
  if (status === "failed") {
    return "danger";
  }
  if (status === "queued" || status === "pending" || status === "canonicalizing" || status === "rebuilding") {
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


export function DashboardPage() {
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

  async function onSubmit(values: MeetingForm) {
    try {
      await createMeeting.mutateAsync(values);
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
  const reviewReadyCount = rows.filter((row) => row.postprocess_status === "review_ready").length;
  const whisperxCount = rows.filter((row) =>
    ["canonicalizing", "transcribing", "aligning", "rebuilding"].includes(row.postprocess_status),
  ).length;

  return (
    <AppShell
      title="Dashboard"
      subtitle={`${session.user?.email ?? ""} hesabı için canlı toplantılar, worker durumu ve transcript operasyonları.`}
      navSlot={
        <button className="nt-btn nt-btn-ghost nt-btn-sm" onClick={() => void session.logout()} type="button">
          Çıkış yap
        </button>
      }
    >
      <section className="nt-metric-grid">
        <MetricCard accent="primary" label="Toplam meeting" value={rows.length} hint="Kayıtlı tüm oturumlar" />
        <MetricCard accent="teal" label="Canlı bot" value={activeCount} hint="joining veya active durumları" />
        <MetricCard accent="warning" label="WhisperX çalışan" value={whisperxCount} hint="canonicalizing / transcribing / aligning / rebuilding" />
        <MetricCard accent="primary" label="Review bekleyen" value={reviewReadyCount} hint="İnline karar bekleyen transcriptler" />
      </section>

      <section className="nt-dashboard-grid">
        <article className="nt-card nt-card-padded">
          <div className="nt-card-head">
            <div>
              <p className="nt-card-label">Yeni meeting</p>
              <h2 className="nt-section-title">Bot oturumu başlat</h2>
            </div>
          </div>
          <form className="nt-form" onSubmit={form.handleSubmit(onSubmit)}>
            <label className="nt-field">
              <span>Toplantı adı</span>
              <input className="nt-input" placeholder="Haftalık ürün senkronu" {...form.register("title")} />
              {form.formState.errors.title ? <small>{form.formState.errors.title.message}</small> : null}
            </label>
            <label className="nt-field">
              <span>Teams linki</span>
              <textarea
                className="nt-input nt-input-mono"
                placeholder="https://teams.microsoft.com/..."
                rows={4}
                {...form.register("teams_link")}
              />
              {form.formState.errors.teams_link ? <small>{form.formState.errors.teams_link.message}</small> : null}
            </label>
            <label className="nt-checkbox">
              <input type="checkbox" {...form.register("audio_recording_enabled")} />
              <span>Ses kaydını da toplamaya çalış</span>
            </label>
            {createMeeting.error ? (
              <div className="nt-alert">{createMeeting.error.message}</div>
            ) : null}
            <button className="nt-btn nt-btn-primary" disabled={createMeeting.isPending} type="submit">
              {createMeeting.isPending ? "Toplantıya katılıyor" : "Oluştur ve katıl"}
            </button>
          </form>
        </article>

        <article className="nt-card nt-card-padded">
          <div className="nt-card-head">
            <div>
              <p className="nt-card-label">Toplantılar</p>
              <h2 className="nt-section-title">Durum görünümü</h2>
            </div>
          </div>
          {meetings.error ? (
            <div className="nt-alert">{meetings.error.message}</div>
          ) : null}
          <div className="nt-meeting-list nt-grid">
            {rows.length === 0 ? (
              <div className="nt-empty-state">
                <strong>Henüz meeting yok</strong>
                <span>İlk kaydı soldaki formdan oluşturabilirsin.</span>
              </div>
            ) : null}
            {rows.map((meeting) => (
              <article className="nt-card nt-meeting-card" key={meeting.id}>
                <div className="nt-meeting-head">
                  <div>
                    <h3 className="nt-meeting-title">{meeting.title}</h3>
                    <p>Oluşturuldu: {formatDate(meeting.created_at)}</p>
                  </div>
                  <div className="nt-meeting-pills">
                    <StatusPill tone={toneForStatus(meeting.status)}>{meeting.status}</StatusPill>
                    <StatusPill tone={toneForStatus(meeting.postprocess_status)}>{meeting.postprocess_status}</StatusPill>
                  </div>
                </div>
                <dl className="nt-meeting-meta">
                  <div>
                    <dt>Ses</dt>
                    <dd>{meeting.audio_status}</dd>
                  </div>
                  <div>
                    <dt>Başladı</dt>
                    <dd>{formatDate(meeting.joined_at)}</dd>
                  </div>
                  <div>
                    <dt>Bitti</dt>
                    <dd>{formatDate(meeting.ended_at)}</dd>
                  </div>
                </dl>
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
                      onClick={() => void joinMeeting.mutateAsync(meeting.id)}
                      type="button"
                    >
                      Katıl
                    </button>
                  ) : null}
                  {meeting.can_stop ? (
                    <button
                      className="nt-btn nt-btn-secondary nt-btn-sm"
                      disabled={stopMeeting.isPending}
                      onClick={() => void stopMeeting.mutateAsync(meeting.id)}
                      type="button"
                    >
                      Durdur
                    </button>
                  ) : null}
                  <Link className="nt-btn nt-btn-ghost nt-btn-sm" to={`/transcripts/${meeting.id}`}>
                    Transcript
                  </Link>
                  <button
                    className="nt-btn nt-btn-danger nt-btn-sm"
                    disabled={deleteMeeting.isPending || meeting.can_stop}
                    onClick={() => void deleteMeeting.mutateAsync(meeting.id)}
                    type="button"
                  >
                    Sil
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
