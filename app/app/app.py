from typing import Optional

import reflex as rx

from .models import Meeting
from .state import (
    DashboardState,
    IndexState,
    ReviewItemEntry,
    State,
    TranscriptEntry,
    TranscriptPageState,
)

BRAND_NAME = "Notera"
BRAND_SUBTITLE = "Canlı Toplantı Notları"
BRAND_DESCRIPTION = (
    "Microsoft Teams toplantıları için canlı transkript, arşiv ve dışa aktarma paneli."
)


# ───── Helpers ─────

def cx(*classes: str) -> str:
    return " ".join(filter(None, classes))


def status_badge_class(status) -> rx.Var:
    return rx.match(
        status,
        ("pending", "nt-badge is-pending"),
        ("joining", "nt-badge is-joining"),
        ("active", "nt-badge is-active"),
        ("completed", "nt-badge is-completed"),
        "nt-badge is-pending",
    )


def status_dot_class(status) -> rx.Var:
    return rx.match(
        status,
        ("pending", "nt-badge-dot is-pending"),
        ("joining", "nt-badge-dot is-joining"),
        ("active", "nt-badge-dot is-active"),
        ("completed", "nt-badge-dot is-completed"),
        "nt-badge-dot is-pending",
    )


def status_copy(status) -> rx.Var:
    return rx.match(
        status,
        ("pending", "Hazır"),
        ("joining", "Bağlanıyor"),
        ("active", "Canlı"),
        ("completed", "Tamamlandı"),
        "Beklemede",
    )


def status_detail(status) -> rx.Var:
    return rx.match(
        status,
        ("pending", "Operasyon başlatılamadı. Gerekirse toplantıyı yeniden ekleyin."),
        ("joining", "Bot toplantı odasına erişiyor."),
        ("active", "Transkript akışı gerçek zamanlı izleniyor."),
        ("completed", "Bu oturum tamamlandı. Aynı toplantı yeniden başlatılmaz."),
        "Durum güncelleniyor.",
    )


def audio_status_copy(status) -> rx.Var:
    return rx.match(
        status,
        ("disabled", "Ses kapalı"),
        ("pending", "Ses hazırlanıyor"),
        ("recording", "Ses kaydediliyor"),
        ("ready", "Ses hazır"),
        ("failed", "Ses kaydı alınamadı"),
        "Ses durumu bilinmiyor",
    )


def audio_status_class(status) -> rx.Var:
    return rx.match(
        status,
        ("disabled", "nt-chip is-muted"),
        ("pending", "nt-chip"),
        ("recording", "nt-chip is-success"),
        ("ready", "nt-chip is-success"),
        ("failed", "nt-chip is-danger"),
        "nt-chip",
    )


def postprocess_status_copy(status) -> rx.Var:
    return rx.match(
        status,
        ("pending", "Doğrulama bekliyor"),
        ("queued", "Doğrulama sırada"),
        ("running", "WhisperX işleniyor"),
        ("transcribing", "WhisperX transcript çıkarıyor"),
        ("canonicalizing", "Teams transcript temizleniyor"),
        ("aligning", "Transcriptler hizalanıyor"),
        ("rebuilding", "Final transcript hazırlanıyor"),
        ("review_ready", "Review hazır"),
        ("completed", "Doğrulama tamamlandı"),
        ("failed", "Doğrulama başarısız"),
        "Doğrulama bilinmiyor",
    )


def review_granularity_copy(granularity) -> rx.Var:
    return rx.match(
        granularity,
        ("word", "Kelime"),
        ("sentence", "Cümle"),
        "Fark",
    )


# ───── Layout Primitives ─────

def app_shell(*children, class_name: str = "", **kwargs) -> rx.Component:
    return rx.box(
        rx.box(class_name="nt-glow-line"),
        rx.box(class_name="nt-bg-gradient"),
        rx.box(*children, class_name=cx("nt-shell", class_name)),
        class_name="nt-app",
        **kwargs,
    )


def card(*children, class_name: str = "") -> rx.Component:
    return rx.box(
        *children,
        class_name=cx("nt-card nt-card-padded", class_name),
    )


# ───── Brand & Nav ─────

def brand_lockup() -> rx.Component:
    return rx.hstack(
        rx.image(
            src="/brand-mark.svg",
            alt=f"{BRAND_NAME} logosu",
            class_name="nt-brand-logo",
        ),
        rx.text(BRAND_NAME, class_name="nt-brand-name"),
        spacing="2",
        align="center",
        class_name="nt-brand",
    )


def shell_nav() -> rx.Component:
    return rx.box(
        rx.hstack(
            brand_lockup(),
            rx.spacer(),
            rx.menu.root(
                rx.menu.trigger(
                    rx.button(
                        rx.icon(tag="user", size=14),
                        rx.text(State.logged_in_email, class_name="nt-menu-email"),
                        class_name="nt-menu-trigger",
                    )
                ),
                rx.menu.content(
                    rx.menu.item(
                        "Çıkış Yap",
                        icon="log-out",
                        on_click=State.logout,
                        color_scheme="red",
                    ),
                    class_name="nt-menu-content",
                    variant="soft",
                    width="200px",
                ),
            ),
            width="100%",
            align="center",
        ),
        class_name="nt-nav",
    )


# ───── Status Indicators ─────

def status_badge(status: str) -> rx.Component:
    return rx.hstack(
        rx.box(class_name=status_dot_class(status)),
        rx.text(status_copy(status), font_size="0.6875rem", font_weight="600"),
        spacing="2",
        align="center",
        class_name=status_badge_class(status),
    )


def inline_chip(text, class_name: str = "nt-chip") -> rx.Component:
    return rx.box(
        rx.text(text, font_size="0.75rem", font_weight="500"),
        class_name=class_name,
    )


# ───── Summary / KPI ─────

def summary_chip(
    label: str,
    value,
    detail: str = "",
    extra: Optional[rx.Component] = None,
) -> rx.Component:
    children = [
        rx.text(label, class_name="nt-kpi-label"),
        rx.text(value, class_name="nt-summary-value"),
    ]
    if isinstance(detail, str):
        if detail:
            children.append(rx.text(detail, class_name="nt-kpi-copy"))
    else:
        children.append(rx.text(detail, class_name="nt-kpi-copy"))
    if extra is not None:
        children.append(extra)

    return rx.box(
        rx.vstack(
            *children,
            spacing="1",
            align_items="start",
            width="100%",
        ),
        class_name="nt-summary-chip",
    )


def stat_card(label: str, value) -> rx.Component:
    return rx.box(
        rx.vstack(
            rx.text(label, class_name="nt-stat-label"),
            rx.heading(value, class_name="nt-stat-value"),
            spacing="1",
            align_items="start",
            width="100%",
        ),
        class_name="nt-stat-card",
    )


# ───── Compact Signal Rows ─────

def compact_signal_row(icon: str, title: str, detail: str) -> rx.Component:
    return rx.hstack(
        rx.center(
            rx.icon(tag=icon, size=14),
            class_name="nt-signal-icon",
        ),
        rx.vstack(
            rx.text(title, class_name="nt-signal-title"),
            rx.text(detail, class_name="nt-signal-desc"),
            spacing="0",
            align_items="start",
        ),
        spacing="3",
        align="start",
        width="100%",
    )


# ───── Empty & Loading States ─────

def skeleton_loader() -> rx.Component:
    return rx.vstack(
        rx.box(width="55%", height="20px", class_name="nt-skeleton"),
        rx.box(width="100%", height="14px", class_name="nt-skeleton"),
        rx.box(width="80%", height="14px", class_name="nt-skeleton"),
        spacing="3",
        width="100%",
        padding="0.75rem 0",
    )


def empty_state(
    icon: str,
    title: str,
    description: str,
    action: Optional[rx.Component] = None,
) -> rx.Component:
    content = [
        rx.center(
            rx.icon(tag=icon, size=28),
            class_name="nt-empty-icon",
        ),
        rx.heading(title, class_name="nt-empty-title"),
        rx.text(description, class_name="nt-empty-desc"),
    ]
    if action is not None:
        content.append(action)

    return rx.box(
        rx.vstack(
            *content,
            spacing="4",
            align="center",
        ),
        class_name="nt-empty",
    )


# ───── Auth Page ─────

def auth_panel() -> rx.Component:
    return rx.box(
        rx.vstack(
            brand_lockup(),
            rx.vstack(
                rx.heading("Hoş geldiniz", class_name="nt-auth-title"),
                rx.text(
                    "Toplantı transkriptlerinize erişmek için giriş yapın.",
                    class_name="nt-auth-desc",
                ),
                spacing="2",
                align_items="start",
                width="100%",
            ),
            rx.vstack(
                rx.text("E-posta adresi", class_name="nt-label"),
                rx.input(
                    type="email",
                    placeholder="ornek@kurum.com",
                    on_change=State.set_email,
                    class_name="nt-input",
                    width="100%",
                    auto_focus=True,
                ),
                spacing="1",
                align_items="start",
                width="100%",
            ),
            rx.vstack(
                rx.button(
                    rx.icon(tag="log-in", size=16),
                    "Giriş Yap",
                    on_click=State.login,
                    class_name="nt-btn nt-btn-primary nt-auth-btn",
                ),
                rx.button(
                    "Kayıt Ol",
                    on_click=State.register,
                    class_name="nt-btn nt-btn-secondary nt-auth-btn",
                ),
                spacing="2",
                width="100%",
            ),
            rx.cond(
                State.error_message != "",
                rx.box(
                    rx.hstack(
                        rx.icon(tag="circle-alert", size=15),
                        rx.text(State.error_message, font_size="0.875rem"),
                        spacing="2",
                        align="center",
                    ),
                    class_name="nt-alert",
                ),
            ),
            spacing="6",
            align_items="start",
            width="100%",
        ),
        class_name="nt-auth-card",
    )


def index() -> rx.Component:
    return app_shell(
        rx.center(
            auth_panel(),
            width="100%",
        ),
        on_mount=IndexState.on_load,
        class_name="nt-auth-page",
    )


# ───── Meeting Setup Dialog ─────

def meeting_setup_dialog(trigger: rx.Component) -> rx.Component:
    return rx.dialog.root(
        rx.dialog.trigger(trigger),
        rx.dialog.content(
            rx.vstack(
                rx.vstack(
                    rx.text("Yeni operasyon", class_name="nt-eyebrow"),
                    rx.heading("Toplantı oluştur", class_name="nt-dialog-title"),
                    rx.text(
                        "Başlık ve Teams bağlantısını girin. Bot toplantıya otomatik katılır.",
                        class_name="nt-dialog-desc",
                    ),
                    spacing="1",
                    align_items="start",
                    width="100%",
                ),
                rx.vstack(
                    rx.text("Toplantı adı", class_name="nt-label"),
                    rx.input(
                        placeholder="Örn: EMEA Weekly Ops Review",
                        on_change=DashboardState.set_new_meeting_title,
                        class_name="nt-input",
                        width="100%",
                    ),
                    spacing="1",
                    align_items="start",
                    width="100%",
                ),
                rx.vstack(
                    rx.text("Teams bağlantısı", class_name="nt-label"),
                    rx.input(
                        placeholder="https://teams.microsoft.com/...",
                        on_change=DashboardState.set_new_meeting_link,
                        class_name="nt-input nt-input-mono",
                        width="100%",
                    ),
                    spacing="1",
                    align_items="start",
                    width="100%",
                ),
                rx.vstack(
                    rx.checkbox(
                        "Ses kaydedilsin mi?",
                        checked=DashboardState.new_meeting_audio_recording_enabled,
                        on_change=DashboardState.set_new_meeting_audio_recording_enabled,
                        class_name="nt-checkbox",
                    ),
                    rx.text(
                        "Açık olduğunda bot Teams sesini kaydetmeyi dener.",
                        class_name="nt-kpi-copy",
                    ),
                    spacing="2",
                    align_items="start",
                    width="100%",
                ),
                rx.grid(
                    compact_signal_row(
                        "bot",
                        "Arka plan ajanı",
                        "Toplantıya bağımsız süreç olarak katılır.",
                    ),
                    compact_signal_row(
                        "messages-square",
                        "Transkript arşivi",
                        "Toplantı sonrası export edilebilir kayıtlar üretir.",
                    ),
                    columns=rx.breakpoints(initial="1", md="2"),
                    spacing="3",
                    width="100%",
                ),
                rx.hstack(
                    rx.dialog.close(
                        rx.button("İptal", class_name="nt-btn nt-btn-secondary")
                    ),
                    rx.dialog.close(
                        rx.button(
                            rx.icon(tag="sparkles", size=15),
                            "Oluştur ve Başlat",
                            on_click=DashboardState.add_meeting,
                            class_name="nt-btn nt-btn-primary",
                        )
                    ),
                    spacing="3",
                    width="100%",
                    justify="end",
                ),
                spacing="5",
                width="100%",
                align_items="start",
            ),
            class_name="nt-dialog",
        ),
    )


# ───── Dashboard ─────

def dashboard_hero() -> rx.Component:
    hero_trigger = meeting_setup_dialog(
        rx.button(
            rx.icon(tag="plus", size=16),
            "Yeni Toplantı",
            class_name="nt-btn nt-btn-primary",
        )
    )

    return rx.box(
        rx.flex(
            # Left: title + description + action
            rx.vstack(
                rx.heading("Operasyon Merkezi", class_name="nt-page-title"),
                rx.text(
                    "Toplantı botları, anlık transkriptler ve arşiv yönetimi.",
                    class_name="nt-hero-copy",
                ),
                rx.hstack(
                    hero_trigger,
                    rx.text(DashboardState.readiness_label, class_name="nt-live-pill"),
                    spacing="3",
                    align="center",
                ),
                spacing="3",
                align_items="start",
                justify="center",
                flex="1 1 auto",
                min_width="0",
            ),
            # Right: compact stats
            rx.box(
                stat_card("Toplam", DashboardState.total_meetings),
                stat_card("Canlı", DashboardState.live_meeting_count),
                stat_card("Arşiv", DashboardState.transcript_entry_count),
                class_name="nt-stats-row",
                flex_shrink="0",
            ),
            direction=rx.breakpoints(initial="column", md="row"),
            gap="1.25rem",
            width="100%",
            align=rx.breakpoints(initial="start", md="center"),
        ),
        class_name="nt-hero",
        width="100%",
    )


def meeting_card(meeting: Meeting) -> rx.Component:
    is_leave_busy = (
        (DashboardState.busy_action == "leave")
        & (DashboardState.busy_meeting_id == meeting.id)
    )

    return card(
        rx.vstack(
            # Top row: status + delete
            rx.hstack(
                status_badge(meeting.status),
                rx.spacer(),
                rx.icon_button(
                    rx.icon(tag="trash-2", size=14),
                    on_click=lambda: DashboardState.delete_meeting(meeting.id),
                    class_name="nt-btn nt-btn-ghost nt-btn-icon",
                    variant="ghost",
                ),
                width="100%",
                align="center",
            ),
            # Title & meta
            rx.vstack(
                rx.heading(meeting.title, class_name="nt-meeting-title"),
                rx.text(meeting.created_at, class_name="nt-meeting-meta"),
                rx.text(meeting.teams_link, class_name="nt-meeting-meta"),
                spacing="1",
                width="100%",
                align_items="start",
            ),
            # Status detail
            rx.text(
                status_detail(meeting.status),
                class_name="nt-kpi-copy",
                width="100%",
            ),
            # Audio & postprocess chips
            rx.vstack(
                rx.hstack(
                    inline_chip(
                        audio_status_copy(meeting.audio_status),
                        class_name=audio_status_class(meeting.audio_status),
                    ),
                    inline_chip(
                        rx.cond(
                            meeting.audio_status == "disabled",
                            "Audio kapalı",
                            rx.cond(
                                meeting.audio_status == "failed",
                                "Teams-only transcript",
                                postprocess_status_copy(meeting.postprocess_status),
                            ),
                        ),
                        class_name="nt-chip",
                    ),
                    spacing="2",
                    width="100%",
                    wrap="wrap",
                ),
                rx.cond(
                    meeting.audio_status == "failed",
                    rx.text(
                        meeting.audio_error,
                        class_name="nt-inline-warning",
                        width="100%",
                    ),
                ),
                spacing="2",
                width="100%",
                align_items="start",
            ),
            # Actions
            rx.hstack(
                rx.cond(
                    (meeting.status == "joining") | (meeting.status == "active"),
                    rx.button(
                        rx.icon(tag="square", size=14),
                        rx.cond(
                            is_leave_busy,
                            "Durduruluyor",
                            "Kaydı Durdur",
                        ),
                        on_click=lambda: DashboardState.leave_meeting(meeting.id),
                        loading=is_leave_busy,
                        class_name="nt-btn nt-btn-danger nt-btn-sm",
                    ),
                ),
                rx.button(
                    rx.icon(tag="messages-square", size=14),
                    "Transkript",
                    on_click=lambda: DashboardState.view_transcripts(meeting.id),
                    class_name="nt-btn nt-btn-secondary nt-btn-sm",
                ),
                spacing="2",
                width="100%",
                class_name="nt-meeting-actions",
            ),
            spacing="3",
            width="100%",
            align_items="start",
        ),
        class_name="nt-meeting-card",
    )


def dashboard() -> rx.Component:
    empty_dialog = meeting_setup_dialog(
        rx.button(
            rx.icon(tag="plus", size=16),
            "İlk Toplantıyı Başlat",
            class_name="nt-btn nt-btn-primary",
        )
    )

    return app_shell(
        shell_nav(),
        rx.container(
            rx.vstack(
                dashboard_hero(),
                rx.vstack(
                    rx.heading(
                        "Aktif ve Arşivlenmiş Oturumlar",
                        class_name="nt-section-title",
                    ),
                    rx.cond(
                        DashboardState.total_meetings > 0,
                        rx.grid(
                            rx.foreach(DashboardState.meetings, meeting_card),
                            class_name="nt-grid",
                        ),
                        empty_state(
                            "calendar-days",
                            "Henüz operasyon kuyruğu yok",
                            "İlk toplantınızı eklediğinizde canlı katılım, transcript yakalama ve export akışları burada görünecek.",
                            action=empty_dialog,
                        ),
                    ),
                    spacing="4",
                    width="100%",
                    align_items="start",
                ),
                spacing="6",
                width="100%",
                align_items="start",
            ),
            width="100%",
            max_width="1480px",
            class_name="nt-container",
        ),
        on_mount=DashboardState.page_mount,
        on_unmount=DashboardState.stop_live_updates,
    )


# ───── Transcript Components ─────

def speaker_badge(entry: TranscriptEntry) -> rx.Component:
    return rx.center(
        rx.text(entry.initials, class_name="nt-speaker-initials"),
        class_name="nt-speaker-badge",
        background=f"var(--{entry.color}-3)",
        color=f"var(--{entry.color}-11)",
        border=f"1px solid var(--{entry.color}-6)",
    )


def transcript_item(entry: TranscriptEntry) -> rx.Component:
    return rx.hstack(
        rx.vstack(
            speaker_badge(entry),
            rx.box(class_name="nt-timeline-line"),
            spacing="0",
            align="center",
            class_name="nt-timeline-col",
        ),
        rx.vstack(
            rx.hstack(
                rx.text(entry.speaker, class_name="nt-stream-speaker"),
                rx.spacer(),
                rx.cond(
                    entry.auto_corrected,
                    inline_chip("Auto düzeltildi", class_name="nt-chip is-success"),
                ),
                rx.text(entry.timestamp, class_name="nt-stream-time"),
                width="100%",
                align="center",
            ),
            rx.box(
                rx.text(entry.text, class_name="nt-stream-text"),
                class_name="nt-stream-bubble",
            ),
            spacing="1",
            align_items="start",
            width="100%",
        ),
        spacing="3",
        align="start",
        width="100%",
        class_name="nt-stream-item",
    )


def transcript_toolbar() -> rx.Component:
    return rx.hstack(
        rx.heading("Transcript", class_name="nt-section-title"),
        width="auto",
        align="center",
        class_name="nt-stream-header",
    )


def review_toolbar() -> rx.Component:
    return rx.hstack(
        rx.heading("Review Kuyruğu", class_name="nt-section-title"),
        rx.spacer(),
        inline_chip(
            rx.hstack(
                rx.text(TranscriptPageState.pending_review_count, font_size="0.75rem"),
                rx.text("bekliyor", font_size="0.75rem"),
                spacing="1",
                align="center",
            ),
        ),
        width="100%",
        align="center",
        class_name="nt-stream-header",
    )


def review_item_card(item: ReviewItemEntry) -> rx.Component:
    return rx.box(
        rx.vstack(
            rx.hstack(
                rx.vstack(
                    rx.text(item.speaker, class_name="nt-stream-speaker"),
                    rx.text(item.timestamp, class_name="nt-stream-time"),
                    spacing="0",
                    align_items="start",
                ),
                rx.spacer(),
                inline_chip(review_granularity_copy(item.granularity)),
                inline_chip(item.confidence_label, class_name="nt-chip is-success"),
                width="100%",
                align="start",
            ),
            rx.vstack(
                rx.text("Mevcut caption", class_name="nt-label"),
                rx.text(item.current_text, class_name="nt-review-text is-current"),
                spacing="1",
                align_items="start",
                width="100%",
            ),
            rx.vstack(
                rx.text("WhisperX metni", class_name="nt-label"),
                rx.text(item.suggested_text, class_name="nt-review-text is-suggested"),
                rx.text(
                    "Uygula derseniz bu metin final transcript'e yazılır.",
                    class_name="nt-kpi-copy",
                ),
                spacing="1",
                align_items="start",
                width="100%",
            ),
            rx.vstack(
                rx.text("Kısa ses klibi", class_name="nt-label"),
                rx.cond(
                    item.has_audio_clip,
                    rx.audio(
                        src=item.audio_clip_src,
                        controls=True,
                        width="100%",
                        class_name="nt-audio-player",
                    ),
                    rx.text(
                        "Ses klibi üretilemedi; metin önerisini yine de karar verebilirsiniz.",
                        class_name="nt-kpi-copy",
                    ),
                ),
                spacing="2",
                align_items="start",
                width="100%",
            ),
            rx.hstack(
                rx.button(
                    "Uygula",
                    on_click=lambda: TranscriptPageState.apply_review_item(item.id),
                    class_name="nt-btn nt-btn-primary nt-btn-sm",
                ),
                rx.button(
                    "Koru",
                    on_click=lambda: TranscriptPageState.keep_review_item(item.id),
                    class_name="nt-btn nt-btn-secondary nt-btn-sm",
                ),
                spacing="3",
                width="100%",
                justify="end",
            ),
            spacing="4",
            width="100%",
            align_items="start",
        ),
        class_name="nt-review-card",
    )


# ───── Bot Preview ─────

def bot_preview_content() -> rx.Component:
    return rx.vstack(
        rx.hstack(
            rx.vstack(
                rx.text("Live Feed", class_name="nt-eyebrow"),
                rx.heading("Bot Önizleme", class_name="nt-section-title"),
                rx.text("Toplantı içinden son kare", class_name="nt-caption"),
                spacing="1",
                align_items="start",
            ),
            rx.spacer(),
            status_badge(TranscriptPageState.meeting_status),
            width="100%",
            align="start",
        ),
        rx.cond(
            TranscriptPageState.has_bot_preview,
            rx.box(
                rx.image(
                    src=TranscriptPageState.bot_preview_src,
                    alt="Bot toplantı ekran görüntüsü",
                    class_name="nt-preview-image",
                ),
                class_name="nt-preview-frame",
            ),
            rx.box(
                rx.center(
                    rx.icon(tag="monitor", size=24),
                    class_name="nt-empty-icon",
                ),
                rx.text(
                    "Bot toplantıdayken alınan son görüntü burada belirecek.",
                    class_name="nt-preview-meta",
                    text_align="center",
                ),
                class_name="nt-preview-empty",
            ),
        ),
        rx.vstack(
            rx.text("Son güncelleme", class_name="nt-label"),
            rx.text(
                TranscriptPageState.bot_preview_label,
                class_name="nt-preview-meta",
            ),
            spacing="1",
            align_items="start",
            width="100%",
        ),
        spacing="4",
        width="100%",
        align_items="start",
    )


def bot_preview_dialog(trigger: rx.Component) -> rx.Component:
    return rx.dialog.root(
        rx.dialog.trigger(trigger),
        rx.dialog.content(
            bot_preview_content(),
            class_name="nt-dialog nt-preview-dialog",
        ),
    )


# ───── Transcript Page ─────

def transcripts_page() -> rx.Component:
    return app_shell(
        shell_nav(),
        rx.container(
            rx.vstack(
                # Header card
                rx.box(
                    rx.vstack(
                        rx.heading(
                            TranscriptPageState.meeting_title,
                            class_name="nt-transcript-page-title",
                        ),
                        rx.flex(
                            rx.hstack(
                                rx.button(
                                    rx.icon(tag="chevron-left", size=14),
                                    "Panele Dön",
                                    on_click=rx.redirect("/dashboard"),
                                    class_name="nt-btn nt-btn-secondary nt-btn-sm",
                                ),
                                spacing="3",
                                align="center",
                            ),
                            rx.hstack(
                                rx.cond(
                                    TranscriptPageState.can_stop_meeting,
                                    rx.button(
                                        rx.icon(tag="square", size=14),
                                        rx.cond(
                                            TranscriptPageState.is_stopping_meeting,
                                            "Durduruluyor",
                                            "Kaydı Durdur",
                                        ),
                                        on_click=TranscriptPageState.leave_current_meeting,
                                        loading=TranscriptPageState.is_stopping_meeting,
                                        class_name="nt-btn nt-btn-danger nt-btn-sm",
                                    ),
                                ),
                                status_badge(TranscriptPageState.meeting_status),
                                bot_preview_dialog(
                                    rx.button(
                                        rx.icon(tag="monitor", size=14),
                                        "Bot",
                                        class_name="nt-btn nt-btn-secondary nt-btn-sm",
                                    )
                                ),
                                rx.button(
                                    rx.icon(tag="file-text", size=14),
                                    "TXT",
                                    on_click=TranscriptPageState.download_txt,
                                    class_name="nt-btn nt-btn-secondary nt-btn-sm",
                                ),
                                rx.button(
                                    rx.icon(tag="table", size=14),
                                    "CSV",
                                    on_click=TranscriptPageState.download_csv,
                                    class_name="nt-btn nt-btn-secondary nt-btn-sm",
                                ),
                                spacing="2",
                                align="center",
                                wrap="wrap",
                            ),
                            direction=rx.breakpoints(initial="column", lg="row"),
                            gap="0.75rem",
                            width="100%",
                            justify="between",
                            align=rx.breakpoints(initial="start", lg="center"),
                        ),
                        rx.grid(
                            summary_chip(
                                "Ses",
                                TranscriptPageState.audio_status_label,
                                TranscriptPageState.audio_status_detail,
                            ),
                            summary_chip(
                                "WhisperX",
                                TranscriptPageState.postprocess_status_label,
                                TranscriptPageState.postprocess_status_detail,
                            ),
                            summary_chip(
                                "Review",
                                TranscriptPageState.pending_review_count,
                                "Bekleyen karar",
                            ),
                            columns=rx.breakpoints(initial="1", md="3"),
                            spacing="3",
                            width="100%",
                        ),
                        # Dedicated audio player area
                        rx.cond(
                            TranscriptPageState.has_master_audio,
                            rx.box(
                                rx.hstack(
                                    rx.center(
                                        rx.icon(tag="audio-lines", size=16),
                                        class_name="nt-signal-icon",
                                    ),
                                    rx.vstack(
                                        rx.text("Toplantı Ses Kaydı", class_name="nt-label"),
                                        rx.text(
                                            TranscriptPageState.master_audio_label,
                                            class_name="nt-caption",
                                        ),
                                        spacing="0",
                                        align_items="start",
                                    ),
                                    spacing="3",
                                    align="center",
                                    width="100%",
                                ),
                                rx.audio(
                                    src=TranscriptPageState.master_audio_src,
                                    controls=True,
                                    width="100%",
                                    class_name="nt-audio-player",
                                ),
                                class_name="nt-audio-section",
                            ),
                        ),
                        rx.cond(
                            TranscriptPageState.has_audio_warning,
                            rx.box(
                                rx.hstack(
                                    rx.icon(tag="triangle-alert", size=14),
                                    rx.text(
                                        TranscriptPageState.audio_status_detail,
                                        font_size="0.875rem",
                                    ),
                                    spacing="2",
                                    align="center",
                                ),
                                class_name="nt-alert",
                                width="100%",
                            ),
                        ),
                        spacing="4",
                        width="100%",
                        align_items="start",
                    ),
                    class_name="nt-transcript-header",
                    width="100%",
                ),
                # Two-column: Transcript + Review
                rx.grid(
                    rx.box(
                        transcript_toolbar(),
                        rx.cond(
                            TranscriptPageState.has_transcripts,
                            rx.auto_scroll(
                                rx.box(
                                    rx.vstack(
                                        rx.foreach(
                                            TranscriptPageState.transcripts,
                                            transcript_item,
                                        ),
                                        spacing="0",
                                        width="100%",
                                    ),
                                    class_name="nt-stream-track",
                                ),
                                class_name="nt-stream-body",
                            ),
                            rx.cond(
                                (TranscriptPageState.meeting_status == "active")
                                | (TranscriptPageState.meeting_status == "joining"),
                                rx.vstack(
                                    skeleton_loader(),
                                    skeleton_loader(),
                                    skeleton_loader(),
                                    spacing="4",
                                    width="100%",
                                    padding="2rem 1.25rem",
                                ),
                                empty_state(
                                    "message-square",
                                    "Henüz transkript yakalanmadı",
                                    "Toplantı başladığında konuşmalar burada gerçek zamanlı akacak.",
                                ),
                            ),
                        ),
                        class_name="nt-stream-shell",
                    ),
                    rx.box(
                        review_toolbar(),
                        rx.cond(
                            TranscriptPageState.has_review_items,
                            rx.box(
                                rx.vstack(
                                    rx.foreach(
                                        TranscriptPageState.review_items,
                                        review_item_card,
                                    ),
                                    spacing="3",
                                    width="100%",
                                ),
                                class_name="nt-review-list",
                            ),
                            empty_state(
                                "shield-check",
                                "Bekleyen review yok",
                                "Kararsız kelime ve cümle farkları oluştuğunda burada ses klibiyle birlikte karar vereceksiniz.",
                            ),
                        ),
                        class_name="nt-review-shell",
                    ),
                    columns=rx.breakpoints(initial="1", xl="2"),
                    spacing="4",
                    width="100%",
                ),
                spacing="5",
                width="100%",
                align_items="start",
            ),
            width="100%",
            max_width="1480px",
            class_name="nt-container",
        ),
        on_mount=TranscriptPageState.page_mount,
        on_unmount=TranscriptPageState.stop_live_updates,
    )


# ───── App Setup ─────

app = rx.App(
    theme=rx.theme(
        appearance="inherit",
        accent_color="iris",
        has_background=True,
    ),
    stylesheets=[
        "https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;600&display=swap",
        "/notera.css",
    ],
)
app.add_page(
    index,
    route="/",
    title=f"{BRAND_NAME} | Giriş",
    description=BRAND_DESCRIPTION,
)
app.add_page(
    dashboard,
    route="/dashboard",
    title=f"{BRAND_NAME} | Toplantılar",
    description=BRAND_DESCRIPTION,
)
app.add_page(
    transcripts_page,
    route="/transcripts/[meeting_id]",
    title=f"{BRAND_NAME} | Transkriptler",
    description=BRAND_DESCRIPTION,
)
