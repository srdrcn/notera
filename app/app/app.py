from urllib.parse import urlparse
from typing import Optional

import reflex as rx

from .models import Meeting
from .state import DashboardState, IndexState, State, TranscriptEntry, TranscriptPageState

BRAND_NAME = "Notera"
BRAND_SUBTITLE = "Canlı Toplantı Notları"
BRAND_DESCRIPTION = (
    "Microsoft Teams toplantıları için canlı transkript, arşiv ve dışa aktarma paneli."
)


def cx(*classes: str) -> str:
    return " ".join(filter(None, classes))


def status_badge_class(status) -> rx.Var:
    return rx.match(
        status,
        ("pending", "ops-status-badge is-pending"),
        ("joining", "ops-status-badge is-joining"),
        ("active", "ops-status-badge is-active"),
        ("completed", "ops-status-badge is-completed"),
        "ops-status-badge is-pending",
    )


def status_dot_class(status) -> rx.Var:
    return rx.match(
        status,
        ("pending", "ops-status-dot is-pending"),
        ("joining", "ops-status-dot is-joining"),
        ("active", "ops-status-dot is-active"),
        ("completed", "ops-status-dot is-completed"),
        "ops-status-dot is-pending",
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


def surface_card(*children, class_name: str = "") -> rx.Component:
    return rx.box(*children, class_name=cx("ops-surface-card", class_name))


def app_shell(*children, class_name: str = "", **kwargs) -> rx.Component:
    return rx.box(
        rx.box(class_name="ops-grid-overlay"),
        rx.box(class_name="ops-orb ops-orb-a"),
        rx.box(class_name="ops-orb ops-orb-b"),
        rx.box(class_name="ops-orb ops-orb-c"),
        rx.box(*children, class_name=cx("ops-shell", class_name)),
        class_name="ops-app",
        **kwargs,
    )


def brand_lockup() -> rx.Component:
    return rx.hstack(
        rx.center(
            rx.image(
                src="/brand-mark.svg",
                alt=f"{BRAND_NAME} logosu",
                class_name="ops-brand-logo",
            ),
            class_name="ops-brand-mark",
        ),
        rx.vstack(
            rx.text(BRAND_NAME, class_name="ops-brand-name"),
            rx.text(BRAND_SUBTITLE, class_name="ops-brand-subtitle"),
            spacing="0",
            align_items="start",
        ),
        spacing="3",
        align="center",
    )


def status_badge(status: str) -> rx.Component:
    return rx.hstack(
        rx.box(class_name=status_dot_class(status)),
        rx.text(status_copy(status), class_name="ops-status-text"),
        spacing="2",
        align="center",
        class_name=status_badge_class(status),
    )


def section_heading(eyebrow: str, title: str, description: str) -> rx.Component:
    return rx.vstack(
        rx.text(eyebrow, class_name="ops-eyebrow"),
        rx.heading(title, class_name="ops-section-title"),
        rx.text(description, class_name="ops-section-copy"),
        spacing="1",
        align_items="start",
    )


def metric_card(icon: str, label: str, value, detail: str) -> rx.Component:
    return surface_card(
        rx.vstack(
            rx.hstack(
                rx.center(
                    rx.icon(tag=icon, size=18),
                    class_name="ops-metric-icon",
                ),
                rx.spacer(),
                rx.text(label, class_name="ops-metric-label"),
                width="100%",
                align="center",
            ),
            rx.heading(value, class_name="ops-metric-value"),
            rx.text(detail, class_name="ops-metric-detail"),
            spacing="3",
            align_items="start",
            width="100%",
        ),
        class_name="ops-metric-card",
    )


def summary_chip(label: str, value, detail: str = "") -> rx.Component:
    children = [
        rx.text(label, class_name="ops-kpi-label"),
        rx.text(value, class_name="ops-summary-value"),
    ]
    if detail:
        children.append(rx.text(detail, class_name="ops-kpi-copy"))

    return rx.box(
        rx.vstack(
            *children,
            spacing="1",
            align_items="start",
            width="100%",
        ),
        class_name="ops-summary-chip",
    )


def info_bullet(icon: str, title: str, body: str) -> rx.Component:
    return surface_card(
        rx.hstack(
            rx.center(
                rx.icon(tag=icon, size=18),
                class_name="ops-signal-icon",
            ),
            rx.vstack(
                rx.text(title, class_name="ops-signal-title"),
                rx.text(body, class_name="ops-signal-copy"),
                spacing="1",
                align_items="start",
            ),
            spacing="3",
            align="start",
            width="100%",
        ),
        class_name="ops-signal-card",
    )


def compact_signal_row(icon: str, title: str, detail: str) -> rx.Component:
    return rx.hstack(
        rx.center(
            rx.icon(tag=icon, size=16),
            class_name="ops-mini-icon",
        ),
        rx.vstack(
            rx.text(title, class_name="ops-mini-title"),
            rx.text(detail, class_name="ops-mini-copy"),
            spacing="0",
            align_items="start",
        ),
        spacing="3",
        align="start",
        width="100%",
        class_name="ops-mini-row",
    )


def empty_state(
    icon: str,
    title: str,
    description: str,
    action: Optional[rx.Component] = None,
) -> rx.Component:
    content = [
        rx.center(
            rx.icon(tag=icon, size=30),
            class_name="ops-empty-icon",
        ),
        rx.heading(title, class_name="ops-empty-title"),
        rx.text(description, class_name="ops-empty-copy"),
    ]
    if action is not None:
        content.append(action)

    return surface_card(
        rx.vstack(
            *content,
            spacing="4",
            align="center",
            class_name="ops-empty-stack",
        ),
        class_name="ops-empty-card",
    )


def shell_nav() -> rx.Component:
    return rx.box(
        rx.hstack(
            brand_lockup(),
            rx.spacer(),
            rx.menu.root(
                rx.menu.trigger(
                    rx.button(
                        rx.icon(tag="user", size=16),
                        rx.text(State.logged_in_email, class_name="ops-menu-email"),
                        class_name="ops-menu-trigger",
                    )
                ),
                rx.menu.content(
                    rx.menu.item(
                        "Çıkış Yap",
                        icon="log-out",
                        on_click=State.logout,
                        color_scheme="red",
                    ),
                    class_name="ops-menu-content",
                    variant="soft",
                    width="220px",
                ),
            ),
            width="100%",
            align="center",
        ),
        class_name="ops-nav-shell",
    )


def meeting_setup_dialog(trigger: rx.Component) -> rx.Component:
    return rx.dialog.root(
        rx.dialog.trigger(trigger),
        rx.dialog.content(
            rx.vstack(
                rx.text("Launch Console", class_name="ops-eyebrow"),
                rx.heading("Yeni toplantı operasyonu aç", class_name="ops-dialog-title"),
                rx.text(
                    "Başlık ve Teams bağlantısını girin. Kaydı oluşturduğunuz anda bot toplantıya katılmak için otomatik başlar.",
                    class_name="ops-dialog-copy",
                ),
                rx.vstack(
                    rx.text("Toplantı adı", class_name="ops-field-label"),
                    rx.input(
                        placeholder="Örn: EMEA Weekly Ops Review",
                        on_change=DashboardState.set_new_meeting_title,
                        class_name="ops-input",
                        width="100%",
                    ),
                    spacing="2",
                    align_items="start",
                    width="100%",
                ),
                rx.vstack(
                    rx.text("Teams toplantı bağlantısı", class_name="ops-field-label"),
                    rx.input(
                        placeholder="https://teams.microsoft.com/...",
                        on_change=DashboardState.set_new_meeting_link,
                        class_name="ops-input ops-input-link",
                        width="100%",
                    ),
                    spacing="2",
                    align_items="start",
                    width="100%",
                ),
                rx.grid(
                    compact_signal_row(
                        "bot",
                        "Arka plan ajanı",
                        "Toplantı eklendiği anda bağımsız süreç olarak katılmayı dener.",
                    ),
                    compact_signal_row(
                        "messages-square",
                        "Transkript arşivi",
                        "Toplantı bittikten sonra export edilebilir kayıtlar üretir.",
                    ),
                    columns=rx.breakpoints(initial="1", md="2"),
                    spacing="3",
                    width="100%",
                ),
                rx.hstack(
                    rx.dialog.close(
                        rx.button("İptal", class_name="ops-secondary-btn ops-dialog-btn")
                    ),
                    rx.dialog.close(
                        rx.button(
                            rx.icon(tag="sparkles", size=16),
                            "Oluştur ve Başlat",
                            on_click=DashboardState.add_meeting,
                            class_name="ops-primary-btn ops-dialog-btn",
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
            class_name="ops-dialog-panel",
        ),
    )


def auth_panel() -> rx.Component:
    return surface_card(
        rx.vstack(
            brand_lockup(),
            rx.heading("Giriş yap", class_name="ops-panel-title"),
            rx.vstack(
                rx.text("E-posta adresi", class_name="ops-field-label"),
                rx.input(
                    type="email",
                    placeholder="ornek@kurum.com",
                    on_change=State.set_email,
                    class_name="ops-input",
                    width="100%",
                    auto_focus=True,
                ),
                spacing="2",
                align_items="start",
                width="100%",
            ),
            rx.grid(
                rx.button(
                    rx.icon(tag="sparkles", size=16),
                    "Giriş Yap",
                    on_click=State.login,
                    class_name="ops-primary-btn ops-auth-btn",
                ),
                rx.button(
                    "Kayıt Ol",
                    on_click=State.register,
                    class_name="ops-secondary-btn ops-auth-btn",
                ),
                columns=rx.breakpoints(initial="1", sm="2"),
                spacing="3",
                width="100%",
            ),
            rx.cond(
                State.error_message != "",
                rx.box(
                    rx.hstack(
                        rx.icon(tag="info", size=16),
                        rx.text(State.error_message, class_name="ops-alert-copy"),
                        spacing="2",
                        align="center",
                    ),
                    class_name="ops-inline-alert",
                ),
            ),
            spacing="5",
            align_items="start",
            width="100%",
        ),
        class_name="ops-auth-card",
    )


def index() -> rx.Component:
    return app_shell(
        rx.container(
            rx.center(
                auth_panel(),
                width="100%",
            ),
            width="100%",
            class_name="ops-auth-container",
        ),
        on_mount=IndexState.on_load,
        class_name="ops-auth-page",
    )


def dashboard_hero() -> rx.Component:
    hero_trigger = meeting_setup_dialog(
        rx.button(
            rx.icon(tag="plus", size=18),
            "Yeni Toplantı",
            class_name="ops-primary-btn ops-hero-btn",
        )
    )

    return surface_card(
        rx.vstack(
            rx.vstack(
                rx.text("Dashboard", class_name="ops-eyebrow"),
                rx.heading("Toplantılar", class_name="ops-page-title"),
                rx.text(
                    DashboardState.operations_summary,
                    class_name="ops-panel-copy",
                ),
                spacing="2",
                align_items="start",
                width="100%",
            ),
            rx.hstack(
                summary_chip("Toplam", DashboardState.total_meetings),
                summary_chip("Canlı", DashboardState.live_meeting_count),
                summary_chip("Arşiv", DashboardState.transcript_entry_count),
                spacing="2",
                width="100%",
                class_name="ops-summary-row",
            ),
            rx.hstack(
                hero_trigger,
                rx.spacer(),
                rx.text(DashboardState.readiness_label, class_name="ops-live-pill"),
                width="100%",
                align="center",
                class_name="ops-hero-actions",
            ),
            spacing="3",
            width="100%",
        ),
        class_name="ops-hero-card ops-dashboard-hero",
    )


def meeting_card(meeting: Meeting) -> rx.Component:
    is_leave_busy = (
        (DashboardState.busy_action == "leave")
        & (DashboardState.busy_meeting_id == meeting.id)
    )

    return surface_card(
        rx.vstack(
            rx.hstack(
                status_badge(meeting.status),
                rx.spacer(),
                rx.icon_button(
                    rx.icon(tag="trash-2", size=15),
                    on_click=lambda: DashboardState.delete_meeting(meeting.id),
                    class_name="ops-icon-btn",
                    variant="ghost",
                ),
                width="100%",
                align="center",
            ),
            rx.vstack(
                rx.heading(meeting.title, class_name="ops-card-title"),
                rx.text(meeting.created_at, class_name="ops-card-host"),
                rx.text(meeting.teams_link, class_name="ops-card-link"),
                spacing="1",
                width="100%",
                align_items="start",
            ),
            rx.text(
                status_detail(meeting.status),
                class_name="ops-kpi-copy",
                width="100%",
            ),
            rx.hstack(
                rx.cond(
                    (meeting.status == "joining") | (meeting.status == "active"),
                        rx.button(
                            rx.icon(tag="activity", size=16),
                            rx.cond(
                                is_leave_busy,
                                "Kayıt Durduruluyor",
                                "Kaydı Durdur",
                            ),
                        on_click=lambda: DashboardState.leave_meeting(meeting.id),
                        loading=is_leave_busy,
                        class_name="ops-danger-btn ops-action-btn",
                    ),
                ),
                rx.button(
                    rx.icon(tag="messages-square", size=16),
                    "Transkript Hub",
                    on_click=lambda: DashboardState.view_transcripts(meeting.id),
                    class_name="ops-secondary-btn ops-action-btn",
                ),
                spacing="2",
                width="100%",
                class_name="ops-action-row",
            ),
            spacing="4",
            width="100%",
            align_items="start",
        ),
        class_name="ops-meeting-card ops-dashboard-meeting-card",
    )


def dashboard() -> rx.Component:
    empty_dialog = meeting_setup_dialog(
        rx.button(
            rx.icon(tag="plus", size=16),
            "İlk Operasyonu Oluştur",
            class_name="ops-primary-btn",
        )
    )

    return app_shell(
        shell_nav(),
        rx.container(
            rx.box(
                rx.box(
                    dashboard_hero(),
                    class_name="ops-dashboard-rail",
                ),
                surface_card(
                    rx.vstack(
                        rx.hstack(
                            rx.heading("Toplantı listesi", class_name="ops-section-title"),
                            width="100%",
                            align="center",
                            class_name="ops-section-row",
                        ),
                        rx.cond(
                            DashboardState.total_meetings > 0,
                            rx.grid(
                                rx.foreach(DashboardState.meetings, meeting_card),
                                columns=rx.breakpoints(initial="1", md="2"),
                                spacing="4",
                                width="100%",
                                class_name="ops-dashboard-grid",
                            ),
                            empty_state(
                                "calendar-days",
                                "Henüz operasyon kuyruğu yok",
                                "İlk toplantınızı eklediğinizde canlı katılım, transcript yakalama ve export akışları bu merkezde görünür olacak.",
                                action=empty_dialog,
                            ),
                        ),
                        spacing="4",
                        width="100%",
                        align_items="start",
                        class_name="ops-dashboard-main",
                    ),
                    class_name="ops-dashboard-main-panel",
                ),
                width="100%",
                class_name="ops-dashboard-layout",
            ),
            width="100%",
            max_width="1520px",
            class_name="ops-page-container",
        ),
        on_mount=DashboardState.page_mount,
        on_unmount=DashboardState.stop_live_updates,
        class_name="ops-dashboard-page",
    )


def speaker_badge(entry: TranscriptEntry) -> rx.Component:
    return rx.center(
        rx.text(entry.initials, class_name="ops-speaker-initials"),
        class_name="ops-speaker-badge",
        background=f"var(--{entry.color}-3)",
        color=f"var(--{entry.color}-11)",
        border=f"1px solid var(--{entry.color}-6)",
        box_shadow=f"0 22px 40px -26px var(--{entry.color}-9)",
    )


def transcript_item(entry: TranscriptEntry) -> rx.Component:
    return rx.hstack(
        rx.vstack(
            speaker_badge(entry),
            rx.box(class_name="ops-timeline-line"),
            spacing="0",
            align="center",
            class_name="ops-timeline-column",
        ),
        rx.vstack(
            rx.hstack(
                rx.text(entry.speaker, class_name="ops-stream-speaker"),
                rx.spacer(),
                rx.text(entry.timestamp, class_name="ops-stream-time"),
                width="100%",
                align="center",
            ),
            rx.box(
                rx.text(entry.text, class_name="ops-stream-text"),
                class_name="ops-stream-bubble",
            ),
            spacing="1",
            align_items="start",
            width="100%",
        ),
        spacing="3",
        align="start",
        width="100%",
        class_name="ops-stream-item",
    )


def transcript_toolbar() -> rx.Component:
    return rx.hstack(
        rx.heading("Transcript", class_name="ops-section-title"),
        width="auto",
        align="center",
        class_name="ops-stream-toolbar",
    )


def bot_preview_content() -> rx.Component:
    return rx.vstack(
        rx.hstack(
            rx.vstack(
                rx.text("Live Feed", class_name="ops-eyebrow"),
                rx.heading("Bot Önizleme", class_name="ops-section-title"),
                rx.text("Toplantı içinden son kare", class_name="ops-bot-preview-subtitle"),
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
                    class_name="ops-bot-preview-image",
                ),
                class_name="ops-bot-preview-frame",
            ),
            rx.box(
                rx.center(
                    rx.icon(tag="monitor", size=28),
                    class_name="ops-empty-icon",
                ),
                rx.heading("Henüz canlı kare yok", class_name="ops-empty-title"),
                rx.text(
                    "Bot toplantıdayken alınan son görüntü burada belirecek.",
                    class_name="ops-empty-copy",
                ),
                class_name="ops-bot-preview-empty",
            ),
        ),
        rx.vstack(
            rx.text("Son güncelleme", class_name="ops-bot-preview-meta-label"),
            rx.text(
                TranscriptPageState.bot_preview_label,
                class_name="ops-bot-preview-meta",
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
            class_name="ops-dialog-panel ops-bot-preview-dialog",
        ),
    )


def transcripts_page() -> rx.Component:
    return app_shell(
        shell_nav(),
        rx.container(
            rx.vstack(
                rx.heading(
                    TranscriptPageState.meeting_title,
                    class_name="ops-page-title ops-transcript-page-heading",
                ),
                surface_card(
                    rx.vstack(
                        rx.flex(
                            rx.hstack(
                                rx.button(
                                    rx.icon(tag="chevron-left", size=16),
                                    "Panele Dön",
                                    on_click=rx.redirect("/dashboard"),
                                    class_name="ops-secondary-btn ops-back-btn",
                                ),
                                spacing="3",
                                align="center",
                                class_name="ops-transcript-topbar-main",
                            ),
                            rx.hstack(
                                rx.cond(
                                    TranscriptPageState.can_stop_meeting,
                                    rx.button(
                                        rx.icon(tag="activity", size=16),
                                        rx.cond(
                                            TranscriptPageState.is_stopping_meeting,
                                            "Kayıt Durduruluyor",
                                            "Kaydı Durdur",
                                        ),
                                        on_click=TranscriptPageState.leave_current_meeting,
                                        loading=TranscriptPageState.is_stopping_meeting,
                                        class_name="ops-danger-btn ops-export-btn ops-stop-btn",
                                    ),
                                ),
                                status_badge(TranscriptPageState.meeting_status),
                                bot_preview_dialog(
                                    rx.button(
                                        rx.icon(tag="monitor", size=16),
                                        "Bot Önizleme",
                                        class_name="ops-secondary-btn ops-export-btn",
                                    )
                                ),
                                rx.button(
                                    rx.icon(tag="file-text", size=16),
                                    "TXT",
                                    on_click=TranscriptPageState.download_txt,
                                    class_name="ops-secondary-btn ops-export-btn",
                                ),
                                rx.button(
                                    rx.icon(tag="table", size=16),
                                    "CSV",
                                    on_click=TranscriptPageState.download_csv,
                                    class_name="ops-secondary-btn ops-export-btn",
                                ),
                                spacing="3",
                                align="center",
                                class_name="ops-transcript-topbar-actions",
                            ),
                            direction=rx.breakpoints(initial="column", lg="row"),
                            gap="1rem",
                            width="100%",
                            justify="between",
                            align=rx.breakpoints(initial="start", lg="center"),
                            class_name="ops-transcript-topbar",
                        ),
                        spacing="3",
                        width="100%",
                        align_items="start",
                    ),
                    class_name="ops-hero-card ops-transcript-header-card",
                ),
                surface_card(
                    transcript_toolbar(),
                    rx.cond(
                        TranscriptPageState.has_transcripts,
                        rx.auto_scroll(
                            rx.box(
                                rx.vstack(
                                    rx.foreach(TranscriptPageState.transcripts, transcript_item),
                                    spacing="0",
                                    width="100%",
                                ),
                                class_name="ops-stream-track",
                            ),
                            class_name="ops-stream-body",
                        ),
                        empty_state(
                            "message-square",
                            "Henüz transcript yakalanmadı",
                            "Toplantı akışı başladığında konuşmacı satırları ve export edilebilir kayıtlar bu timeline içinde görünecek.",
                        ),
                    ),
                    class_name="ops-stream-shell ops-transcript-shell",
                ),
                spacing="6",
                width="100%",
                align_items="start",
                class_name="ops-transcript-stack",
            ),
            width="100%",
            max_width="1520px",
            class_name="ops-page-container ops-transcript-container",
        ),
        on_mount=TranscriptPageState.page_mount,
        on_unmount=TranscriptPageState.stop_live_updates,
        class_name="ops-transcript-page",
    )


app = rx.App(
    theme=rx.theme(
        appearance="inherit",
        accent_color="blue",
        has_background=True,
    ),
    stylesheets=[
        "https://fonts.googleapis.com/css2?family=Instrument+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;600&family=Space+Grotesk:wght@500;700&display=swap",
        "/premium.css",
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
