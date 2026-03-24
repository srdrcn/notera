from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path

from backend.runtime.teams_links import TEAMS_JOIN_WITH_ID_PAGE_URL, parse_join_with_id_target
from backend.workers.bot_participants import PARTICIPANT_REGISTRY_CONFIG
from backend.workers.bot_store import resolve_meeting_owner_user_id


logger = logging.getLogger("notera.worker.bot")
REPO_ROOT = Path(__file__).resolve().parents[2]
CHAT_NOTICE_MESSAGE = (
    "Konuşmaları kayıt altına almaya başladım. "
    "Beni toplantıdan çıkarmak için chat'e sadece 'bot ok' yazabilirsiniz."
)
CHAT_LANGUAGE_HELP_MESSAGE = (
    "Caption dilini değiştirmek isterseniz Teams içinde şu yolu izleyin: "
    "More -> Language and speech -> Show live captions. "
    "Ardından caption alanındaki ayar simgesinden Language settings veya Caption settings bölümünü açıp Language alanını değiştirin."
)
CHAT_AUDIO_FAILURE_MESSAGE = "Ses kaydı başlatılamadı."
CHAT_EXIT_COMMAND = "bot ok"
CHAT_EXIT_ACK_MESSAGE = "Çıkış komutu algılandı. Toplantıdan çıkılıyor."


async def get_first_visible_locator(page, selectors, timeout: int = 1000):
    for selector in selectors:
        try:
            locator = page.locator(selector)
            count = await locator.count()
            for idx in range(count):
                candidate = locator.nth(idx)
                if not await candidate.is_visible(timeout=timeout):
                    continue
                return candidate, selector
        except Exception as exc:
            logger.debug("Failed searching selector %s: %s", selector, exc)
    return None, None


async def click_first_visible_selector(page, selectors, click_label: str, wait_after: int = 1, dump_filename: str | None = None) -> bool:
    for selector in selectors:
        candidate, _ = await get_first_visible_locator(page, [selector], timeout=1000)
        if candidate is None:
            continue
        try:
            await candidate.click(force=True, timeout=5000)
            logger.info("Clicked %s with selector: %s", click_label, selector)
            if wait_after:
                await asyncio.sleep(wait_after)
            if dump_filename:
                await dump_dom(page, dump_filename)
            return True
        except Exception as exc:
            logger.debug("Failed clicking %s with selector %s: %s", click_label, selector, exc)
    return False


async def open_more_menu(page) -> bool:
    selectors = [
        "button#callingButtons-showMoreBtn",
        "button[aria-label='More']",
        "button[title='More options']",
        "button[aria-label='More options']",
        "button:has-text('More')",
    ]
    if await click_first_visible_selector(
        page,
        selectors,
        "More button",
        wait_after=1,
        dump_filename="more_menu_dom.html",
    ):
        return True
    logger.warning("Could not open the Teams More menu in the bot session.")
    return False


async def participant_panel_visible(page) -> bool:
    selectors = PARTICIPANT_REGISTRY_CONFIG["panel_selectors"]
    for selector in selectors:
        try:
            if not await page.locator(selector).first.is_visible(timeout=250):
                continue
            if selector == "[data-tid*='right-side-panel']":
                header_text = await page.evaluate(
                    """() => {
                      const root = document.querySelector('[data-tid="calling-right-side-panel"]');
                      const header = root?.querySelector?.('[data-tid="right-side-panel-header-title"], [role="heading"], h1, h2, h3');
                      return (header?.innerText || header?.getAttribute?.('aria-label') || header?.getAttribute?.('title') || '').trim();
                    }"""
                )
                if not re.search(r"participant|people|roster|katılımc|kisi|kişi", (header_text or ""), re.IGNORECASE):
                    continue
                return True
        except Exception:
            continue

    try:
        return await page.evaluate(
            """() => {
              const isVisible = (node) => {
                if (!node) return false;
                const style = window.getComputedStyle(node);
                const rect = node.getBoundingClientRect();
                return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
              };
              const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim().toLowerCase();
              const hasParticipantHint = (value) => /participant|people|roster|attendee|member|guest|katılımc|kisi|kişi/.test(value);
              const candidates = Array.from(document.querySelectorAll('[role="dialog"], aside, [role="complementary"], section, div'));
              for (const node of candidates) {
                if (!isVisible(node)) continue;
                const ownText = normalize([
                  node.getAttribute('data-tid'),
                  node.getAttribute('aria-label'),
                  node.getAttribute('title'),
                ].filter(Boolean).join(' '));
                const headingText = normalize(
                  (node.querySelector('[data-tid="right-side-panel-header-title"], h1, h2, h3, [role="heading"]')?.innerText || '')
                );
                if (!hasParticipantHint(`${ownText} ${headingText}`)) continue;
                const hasRows = node.querySelector(
                  '[role="listitem"], [role="row"], [role="treeitem"], [role="option"], [data-tid*="participant"], [data-tid*="persona"], [data-tid*="roster"], [class*="participant"], [class*="roster"], [class*="persona"]'
                );
                if (hasRows || headingText) {
                  return true;
                }
              }
              return false;
            }"""
        )
    except Exception:
        return False


async def open_participant_panel(page) -> bool:
    if await participant_panel_visible(page):
        return True
    selectors = PARTICIPANT_REGISTRY_CONFIG["panel_button_selectors"]
    if await click_first_visible_selector(page, selectors, "participant panel", wait_after=1):
        return await participant_panel_visible(page)
    try:
        clicked = await page.evaluate(
            """() => {
              const isVisible = (node) => {
                if (!node) return false;
                const style = window.getComputedStyle(node);
                const rect = node.getBoundingClientRect();
                return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
              };
              const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim().toLowerCase();
              const scoreFor = (value) => {
                let score = 0;
                if (/participants?/.test(value)) score += 6;
                if (/people/.test(value)) score += 5;
                if (/roster/.test(value)) score += 4;
                if (/member|attendee|guest/.test(value)) score += 3;
                if (/chat and people/.test(value)) score += 2;
                if (/katılımc|kisi|kişi/.test(value)) score += 4;
                if (/chat/.test(value) && !/people|participant/.test(value)) score -= 3;
                if (/more/.test(value)) score -= 2;
                return score;
              };
              let best = null;
              let bestScore = 0;
              for (const node of Array.from(document.querySelectorAll('button, [role="button"], [role="tab"]'))) {
                if (!isVisible(node)) continue;
                const text = normalize([
                  node.innerText,
                  node.getAttribute('aria-label'),
                  node.getAttribute('title'),
                  node.getAttribute('data-tid'),
                  node.id,
                ].filter(Boolean).join(' '));
                const score = scoreFor(text);
                if (score > bestScore) {
                  best = node;
                  bestScore = score;
                }
              }
              if (!best || bestScore <= 0) return false;
              best.click();
              return true;
            }"""
        )
        if clicked:
            logger.info("Opened participant panel via generic button scan.")
            await asyncio.sleep(1)
            return await participant_panel_visible(page)
    except Exception as exc:
        logger.debug("Generic participant panel opener failed: %s", exc)
    return False


async def open_language_and_speech_menu(page) -> bool:
    selectors = [
        "[role='menuitem']:has-text('Language and speech')",
        "button:has-text('Language and speech')",
        "[role='button']:has-text('Language and speech')",
        "[aria-label='Language and speech']",
        "[title='Language and speech']",
    ]
    return await click_first_visible_selector(
        page,
        selectors,
        "Language and speech entry",
        wait_after=1,
        dump_filename="language_and_speech_menu_dom.html",
    )


async def select_computer_audio(page) -> bool:
    selectors = [
        "button:has-text('Computer audio')",
        "[role='button']:has-text('Computer audio')",
        "[role='radio']:has-text('Computer audio')",
        "[data-tid='prejoin-join-audio-option-computer-audio']",
        "[data-tid='prejoin-audio-option-computer-audio']",
        "button:has-text('Audio')",
    ]
    if await click_first_visible_selector(
        page,
        selectors,
        "Computer audio",
        wait_after=1,
        dump_filename="computer_audio_dom.html",
    ):
        return True
    try:
        return await page.evaluate("""() => {
            const candidates = Array.from(document.querySelectorAll('button, [role="button"], [role="radio"]'));
            const target = candidates.find((element) => {
                const text = (element.innerText || element.getAttribute('aria-label') || '').trim();
                return /computer audio/i.test(text);
            });
            if (!target) return false;
            target.click();
            return true;
        }""")
    except Exception as exc:
        logger.debug("Could not select computer audio via page evaluation: %s", exc)
        return False


async def launch_teams_browser(playwright):
    launch_options = {
        "headless": True,
        "args": ["--autoplay-policy=no-user-gesture-required"],
    }
    try:
        browser = await playwright.chromium.launch(channel="msedge", **launch_options)
        logger.info("Launched Teams bot with Playwright msedge channel.")
        return browser
    except Exception as exc:
        logger.warning("msedge channel launch failed, falling back to bundled chromium: %s", exc)
        return await playwright.chromium.launch(**launch_options)


async def open_meeting_entry(page, meeting_url: str) -> None:
    meeting_by_id = parse_join_with_id_target(meeting_url)
    if not meeting_by_id:
        await page.goto(meeting_url)
        return

    meeting_id, passcode = meeting_by_id
    logger.info("Navigating to Teams join-by-ID page for meeting %s", meeting_id)
    await page.goto(TEAMS_JOIN_WITH_ID_PAGE_URL)

    meeting_id_input = None
    for locator in [
        page.get_by_label(re.compile("meeting id", re.IGNORECASE)).first,
        page.get_by_placeholder(re.compile("meeting id", re.IGNORECASE)).first,
        page.locator("input[name*='meeting']").first,
    ]:
        try:
            await locator.wait_for(state="visible", timeout=5000)
            meeting_id_input = locator
            break
        except Exception:
            continue
    if meeting_id_input is None:
        raise RuntimeError("Teams join-by-ID formunda meeting ID alani bulunamadi.")
    await meeting_id_input.fill(meeting_id)

    passcode_input = None
    for locator in [
        page.get_by_label(re.compile("passcode", re.IGNORECASE)).first,
        page.get_by_placeholder(re.compile("passcode", re.IGNORECASE)).first,
        page.locator("input[name*='passcode']").first,
        page.locator("input[type='password']").first,
    ]:
        try:
            await locator.wait_for(state="visible", timeout=5000)
            passcode_input = locator
            break
        except Exception:
            continue
    if passcode_input is None:
        raise RuntimeError("Teams join-by-ID formunda passcode alani bulunamadi.")
    await passcode_input.fill(passcode)

    join_button = page.get_by_role("button", name=re.compile("join a meeting", re.IGNORECASE)).first
    await join_button.wait_for(state="visible", timeout=10000)
    await join_button.click(timeout=10000)
    await page.wait_for_load_state("networkidle")


async def has_caption_surface(page) -> bool:
    try:
        return await page.evaluate("""() => {
            const selectors = [
                '[data-tid="closed-caption-renderer-wrapper"]',
                '[data-tid="closed-caption-text"]',
                '[role="log"]',
            ];
            return selectors.some((selector) => document.querySelector(selector));
        }""")
    except Exception as exc:
        logger.debug("Caption surface check failed: %s", exc)
        return False


async def try_enable_live_captions(page) -> bool:
    if await has_caption_surface(page):
        return True

    logger.info("No live caption DOM detected yet. Trying to enable captions in the bot session...")
    if not await open_more_menu(page):
        return False

    if await open_language_and_speech_menu(page):
        logger.info("Opened Language and speech submenu before enabling live captions.")
    else:
        logger.info("Language and speech submenu was not visible. Falling back to direct captions entry.")

    caption_selectors = [
        "[role='menuitem']:has-text('Show live captions')",
        "button:has-text('Show live captions')",
        "[title='Show live captions']",
        "#closed-captions-button",
        "[role='menuitem'][id='closed-captions-button']",
        "[role='menuitem'][aria-label='Captions']",
    ]

    for selector in caption_selectors:
        try:
            item = page.locator(selector)
            count = await item.count()
            for idx in range(count):
                candidate = item.nth(idx)
                if not await candidate.is_visible(timeout=1000):
                    continue

                title = await candidate.get_attribute("title") or ""
                if "Hide live captions" in title:
                    logger.info("Live captions are already enabled in the bot session.")
                    await asyncio.sleep(1)
                    return await has_caption_surface(page)

                await candidate.click(force=True, timeout=5000)
                logger.info("Clicked live captions entry with selector: %s", selector)
                await asyncio.sleep(2)
                await dump_dom(page, "live_captions_enabled_dom.html")
                return await has_caption_surface(page)
        except Exception as exc:
            logger.debug("Failed clicking captions menu item with selector %s: %s", selector, exc)

    logger.warning("Could not find a visible Captions menu item in the Teams More menu.")
    await dump_dom(page, "live_captions_entry_not_found_dom.html")
    return False


async def handle_prejoin_popups(page, allow_without_audio: bool = False) -> bool:
    try:
        permission_buttons = [
            "button:has-text('Continue on this browser')",
            "button:has-text('Allow')",
            "button:has-text('Dismiss')",
            "button:has-text('Got it')",
        ]
        if allow_without_audio:
            permission_buttons = [
                "button:has-text('Continue without audio or video')",
                "button[aria-label*='Continue without']",
                "button[data-tid*='continue-without']",
            ] + permission_buttons

        for selector in permission_buttons:
            try:
                button = page.locator(selector)
                count = await button.count()
                if count <= 0:
                    continue
                for index in range(count):
                    candidate = button.nth(index)
                    if await candidate.is_visible(timeout=2000):
                        logger.info("Clicking permission/blocker button: %s", selector)
                        await candidate.click(force=True, timeout=10000)
                        await asyncio.sleep(2)
                        return True
            except Exception:
                continue
    except Exception as exc:
        logger.debug("Permission popup check failure: %s", exc)
    return False


async def wait_for_join_name_input(page, timeout_ms: int = 60000):
    selectors = [
        "input[placeholder='Type your name']",
        "input[data-tid='prejoin-display-name-input']",
        "input[name='displayName']",
        "input.input-field",
    ]

    try:
        await page.wait_for_selector("input", timeout=timeout_ms)
    except Exception as exc:
        logger.error("Page did not load any input within 60s: %s", exc)
        return None

    for selector in selectors:
        try:
            name_input = page.locator(selector)
            if await name_input.is_visible(timeout=2000):
                logger.info("Found name input with selector: %s", selector)
                return name_input
        except Exception:
            continue

    logger.warning("Preferred selectors failed. Using first visible input.")
    return page.locator("input").first


async def click_join_now_button(page, audio_recording_enabled: bool) -> bool:
    join_buttons = [
        "button:has-text('Join now')",
        "button[data-tid='prejoin-join-button']",
        "button.primary-button",
        "button[type='button']:has-text('Join')",
    ]

    for _ in range(45):
        await handle_prejoin_popups(page, allow_without_audio=not audio_recording_enabled)
        for selector in join_buttons:
            try:
                button = page.locator(selector)
                if await button.is_visible(timeout=500):
                    await button.click(timeout=2000)
                    logger.info("Clicked join button with selector: %s", selector)
                    return True
            except Exception:
                continue
        await asyncio.sleep(1)

    logger.error("Could not find a Join button after 45s.")
    return False


async def complete_prejoin_join(page, audio_recording_enabled: bool, bot_display_name: str = "Transcription Bot") -> bool:
    await handle_prejoin_popups(page, allow_without_audio=not audio_recording_enabled)

    name_input = await wait_for_join_name_input(page)
    if name_input is None or not await name_input.is_visible(timeout=500):
        return False

    await name_input.fill(bot_display_name)
    logger.info("Name filled.")

    await handle_prejoin_popups(page, allow_without_audio=not audio_recording_enabled)
    if audio_recording_enabled:
        if await select_computer_audio(page):
            logger.info("Computer audio selected successfully.")
        else:
            logger.warning("Computer audio selection did not succeed; audio capture may fail.")
            await handle_prejoin_popups(page, allow_without_audio=True)
    else:
        await handle_prejoin_popups(page, allow_without_audio=True)

    return await click_join_now_button(page, audio_recording_enabled)


async def is_chat_compose_visible(page) -> bool:
    selectors = [
        "div[data-tid='ckeditor'][contenteditable='true']",
        "[data-tid='chat-pane-compose-message-footer']",
        "button[data-tid='newMessageCommands-send']",
    ]
    for selector in selectors:
        try:
            locator = page.locator(selector)
            count = await locator.count()
            for idx in range(count):
                candidate = locator.nth(idx)
                if await candidate.is_visible(timeout=500):
                    return True
        except Exception as exc:
            logger.debug("Chat compose visibility check failed for %s: %s", selector, exc)
    return False


async def ensure_chat_panel_open(page) -> bool:
    if await is_chat_compose_visible(page):
        return True

    chat_button_selectors = [
        "button#chat-button",
        "button[aria-label='Chat']",
        "button:has-text('Chat')",
    ]
    for selector in chat_button_selectors:
        try:
            locator = page.locator(selector)
            count = await locator.count()
            for idx in range(count):
                candidate = locator.nth(idx)
                if not await candidate.is_visible(timeout=1000):
                    continue
                await candidate.click(force=True, timeout=5000)
                logger.info("Clicked chat button with selector: %s", selector)
                await asyncio.sleep(2)
                if await is_chat_compose_visible(page):
                    return True
        except Exception as exc:
            logger.debug("Failed opening chat panel with selector %s: %s", selector, exc)
    logger.warning("Could not open the Teams chat panel.")
    return False


async def send_chat_message(page, message: str) -> bool:
    if not await ensure_chat_panel_open(page):
        return False

    editor_selectors = [
        "div[data-tid='ckeditor'][contenteditable='true']",
        "[role='textbox'][contenteditable='true'][data-tid='ckeditor']",
    ]
    editor = None
    for selector in editor_selectors:
        try:
            locator = page.locator(selector)
            count = await locator.count()
            for idx in range(count):
                candidate = locator.nth(idx)
                if await candidate.is_visible(timeout=1000):
                    editor = candidate
                    break
            if editor is not None:
                break
        except Exception as exc:
            logger.debug("Failed locating chat editor with selector %s: %s", selector, exc)

    if editor is None:
        logger.warning("Could not find a visible Teams chat editor.")
        return False

    try:
        await editor.click(timeout=5000)
        await editor.fill(message)
    except Exception as exc:
        logger.warning("Could not fill Teams chat editor directly: %s", exc)
        try:
            await editor.click(timeout=5000)
            await page.keyboard.press("Meta+A")
            await page.keyboard.type(message)
        except Exception as keyboard_error:
            logger.error("Could not type the Teams chat announcement: %s", keyboard_error)
            return False

    send_selectors = [
        "button[data-tid='newMessageCommands-send']",
        "button[name='send']",
        "button[title*='Send']",
    ]
    for selector in send_selectors:
        try:
            locator = page.locator(selector)
            count = await locator.count()
            for idx in range(count):
                candidate = locator.nth(idx)
                if not await candidate.is_visible(timeout=1000):
                    continue
                await candidate.click(force=True, timeout=5000)
                logger.info("Message sent to Teams chat: %s", message)
                return True
        except Exception as exc:
            logger.debug("Failed clicking send button with selector %s: %s", selector, exc)

    try:
        await editor.press("Meta+Enter")
        logger.info("Message sent to Teams chat via keyboard shortcut: %s", message)
        return True
    except Exception as exc:
        logger.error("Could not send the Teams chat announcement: %s", exc)
        return False


async def send_chat_notice(page) -> bool:
    return await send_chat_message(page, CHAT_NOTICE_MESSAGE)


async def send_chat_language_help_notice(page) -> bool:
    return await send_chat_message(page, CHAT_LANGUAGE_HELP_MESSAGE)


def normalize_chat_command_text(text: str | None) -> str:
    normalized = (text or "").casefold()
    return " ".join(normalized.split())


def is_exit_command_message(text: str | None) -> bool:
    return normalize_chat_command_text(text) == CHAT_EXIT_COMMAND


async def get_chat_messages(page) -> list[dict[str, str]]:
    if not await ensure_chat_panel_open(page):
        return []
    try:
        return await page.evaluate("""() => {
            return Array.from(document.querySelectorAll('[data-tid="chat-pane-message"]'))
                .map((messageNode) => {
                    const item = messageNode.closest('[data-tid="chat-pane-item"]') || messageNode.parentElement;
                    const authorNode = item?.querySelector('[data-tid="message-author-name"]');
                    const contentNode = messageNode.querySelector('[id^="content-"]');
                    const mid = messageNode.getAttribute('data-mid') || messageNode.id || '';
                    const author = authorNode?.innerText?.trim() || '';
                    const text =
                        contentNode?.innerText?.trim() ||
                        contentNode?.getAttribute('aria-label')?.trim() ||
                        messageNode.getAttribute('aria-label')?.trim() ||
                        '';
                    return { mid, author, text };
                })
                .filter((message) => message.mid && message.text);
        }""")
    except Exception as exc:
        logger.debug("Could not read Teams chat messages: %s", exc)
        return []


async def detect_exit_command(page, known_message_ids: set[str]) -> tuple[bool, set[str]]:
    messages = await get_chat_messages(page)
    if not messages:
        return False, known_message_ids

    updated_ids = set(known_message_ids)
    for message in messages:
        mid = message.get("mid", "")
        author = (message.get("author") or "").strip()
        text = (message.get("text") or "").strip()

        if not mid or mid in updated_ids:
            continue
        updated_ids.add(mid)

        if not text or author == "Transcription Bot":
            continue
        if is_exit_command_message(text):
            logger.info("Chat exit command detected from '%s': %s", author or "Unknown", text)
            return True, updated_ids
    return False, updated_ids


async def leave_meeting_via_ui(page) -> bool:
    leave_selectors = [
        "button#hangup-button",
        "[data-tid='call-end-button']",
        "button[title*='Leave']",
        "button[aria-label*='Leave']",
        "button[title*='Ayrıl']",
        "button[aria-label*='Ayrıl']",
        "button:has-text('Leave')",
        "button:has-text('Ayrıl')",
        "button:has-text('Hang up')",
    ]
    for selector in leave_selectors:
        try:
            locator = page.locator(selector)
            count = await locator.count()
            for idx in range(count):
                candidate = locator.nth(idx)
                if not await candidate.is_visible(timeout=1000):
                    continue
                await candidate.click(force=True, timeout=5000)
                logger.info("Clicked leave button with selector: %s", selector)
                await asyncio.sleep(1)
                return True
        except Exception as exc:
            logger.debug("Failed clicking leave button with selector %s: %s", selector, exc)

    try:
        clicked = await page.evaluate("""() => {
            const candidates = Array.from(document.querySelectorAll('button, [role="button"]'));
            const target = candidates.find((element) => {
                const text = (
                    element.innerText ||
                    element.getAttribute('aria-label') ||
                    element.getAttribute('title') ||
                    ''
                ).trim();
                return /leave|hang up|ayrıl/i.test(text);
            });
            if (!target) return false;
            target.click();
            return true;
        }""")
        if clicked:
            logger.info("Clicked leave button via DOM text fallback.")
            await asyncio.sleep(1)
            return True
    except Exception as exc:
        logger.debug("Failed leave button DOM fallback: %s", exc)

    logger.warning("Could not find a visible Leave button before shutdown.")
    return False


def get_live_meeting_screenshot_path(meeting_id: int) -> str:
    screenshot_root = os.getenv("NOTERA_LIVE_PREVIEW_ROOT")
    if screenshot_root:
        screenshot_dir = os.path.abspath(screenshot_root)
    else:
        screenshot_dir = os.path.abspath(os.path.join(REPO_ROOT, "data", "live_previews"))
    os.makedirs(screenshot_dir, exist_ok=True)
    user_id = resolve_meeting_owner_user_id(meeting_id)
    return os.path.join(screenshot_dir, f"user_{user_id}_meeting_{meeting_id}.png")


def delete_live_meeting_screenshot(screenshot_path: str | None) -> None:
    if not screenshot_path:
        return
    try:
        os.remove(screenshot_path)
        logger.info("Deleted live meeting screenshot asset.")
    except FileNotFoundError:
        return
    except Exception as exc:
        logger.debug("Could not delete live meeting screenshot asset: %s", exc)


async def take_periodic_screenshot(page, stop_event: asyncio.Event, screenshot_path: str) -> None:
    while not stop_event.is_set():
        try:
            await page.screenshot(path=screenshot_path)
            logger.info("Periodic live preview screenshot updated.")
        except Exception as exc:
            logger.error("Failed to take periodic screenshot: %s", exc)
        await asyncio.sleep(10)


def get_bot_debug_path(filename: str) -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)


def build_timestamped_debug_filename(filename: str) -> str:
    stem, ext = os.path.splitext(filename)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    return f"{stem}_{timestamp}{ext}"


async def collect_debug_summary(page) -> dict[str, object]:
    try:
        return await page.evaluate(
            """() => {
                const normalize = (value) =>
                    (value || "").replace(/\\s+/g, " ").trim();
                const isVisible = (el) => {
                    if (!el) {
                        return false;
                    }
                    const style = window.getComputedStyle(el);
                    const rect = el.getBoundingClientRect();
                    return (
                        style.display !== "none" &&
                        style.visibility !== "hidden" &&
                        rect.width > 0 &&
                        rect.height > 0
                    );
                };
                const collectTexts = (selector, limit = 30) => {
                    const values = [];
                    for (const el of Array.from(document.querySelectorAll(selector))) {
                        if (!isVisible(el)) {
                            continue;
                        }
                        const value = normalize(
                            el.innerText ||
                            el.getAttribute("aria-label") ||
                            el.getAttribute("title") ||
                            ""
                        );
                        if (!value || values.includes(value)) {
                            continue;
                        }
                        values.push(value);
                        if (values.length >= limit) {
                            break;
                        }
                    }
                    return values;
                };

                const bodyText = normalize(document.body?.innerText || "");
                const activeElement = document.activeElement;
                const activeText = normalize(
                    activeElement?.innerText ||
                    activeElement?.getAttribute?.("aria-label") ||
                    activeElement?.getAttribute?.("title") ||
                    activeElement?.id ||
                    activeElement?.tagName ||
                    ""
                );

                return {
                    url: window.location.href,
                    title: document.title,
                    active_element: activeText,
                    phrases: {
                        language_and_speech: bodyText.includes("Language and speech"),
                        show_live_captions: bodyText.includes("Show live captions"),
                        language_settings: bodyText.includes("Language settings"),
                        spoken_language_in_meeting: bodyText.includes("Spoken language in this meeting"),
                        caption_language: bodyText.includes("Caption language"),
                        translate_hint: bodyText.includes("Translate your transcript and captions to this language."),
                        confirmation_dialog: bodyText.includes("Is this the language that everyone is speaking?"),
                        update_button: bodyText.includes("Update"),
                    },
                    visible: {
                        headings: collectTexts("h1, h2, h3, [data-tid='right-side-panel-header-title']", 20),
                        menuitems: collectTexts("[role='menuitem']", 40),
                        buttons: collectTexts("button", 50),
                        options: collectTexts("[role='option'], [role='menuitemradio']", 40),
                        comboboxes: collectTexts("[role='combobox']", 20),
                    },
                    data_tids: Array.from(
                        new Set(
                            Array.from(document.querySelectorAll("[data-tid]"))
                                .map((el) => el.getAttribute("data-tid"))
                                .filter(Boolean)
                        )
                    ).slice(0, 150),
                    body_text_excerpt: bodyText.slice(0, 4000),
                };
            }"""
        )
    except Exception as exc:
        return {"summary_error": str(exc)}


async def dump_dom(page, filename: str = "debug_dom.html") -> bool:
    return False
