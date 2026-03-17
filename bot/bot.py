import asyncio
import logging
import os
import signal
import sqlite3
from datetime import datetime
from playwright.async_api import async_playwright

# Setup logging
log_format = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(
    level=logging.INFO,
    format=log_format,
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("TeamsBot")
CHAT_NOTICE_MESSAGE = (
    "Konuşmaları kayıt altına almaya başladım. Live caption dilinizi düzgün algılama için istediğiniz dil ile değiştirin. "
    "Beni toplantıdan çıkarmak için chat'e sadece 'bot ok' yazabilirsiniz."
)
CHAT_EXIT_COMMAND = "bot ok"
CHAT_EXIT_ACK_MESSAGE = "Çıkış komutu algılandı. Toplantıdan çıkılıyor."


async def click_first_visible_selector(
    page,
    selectors,
    click_label,
    wait_after=1,
    dump_filename=None,
):
    """Click the first visible element matching the provided selectors."""
    for selector in selectors:
        try:
            locator = page.locator(selector)
            count = await locator.count()
            for idx in range(count):
                candidate = locator.nth(idx)
                if not await candidate.is_visible(timeout=1000):
                    continue
                await candidate.click(force=True, timeout=5000)
                logger.info(f"Clicked {click_label} with selector: {selector}")
                if wait_after:
                    await asyncio.sleep(wait_after)
                if dump_filename:
                    await dump_dom(page, dump_filename)
                return True
        except Exception as e:
            logger.debug(f"Failed clicking {click_label} with selector {selector}: {e}")
    return False


async def open_more_menu(page):
    """Open the Teams in-meeting More menu."""
    more_button_selectors = [
        "button#callingButtons-showMoreBtn",
        "button[aria-label='More']",
        "button[title='More options']",
        "button[aria-label='More options']",
        "button:has-text('More')",
    ]

    if await click_first_visible_selector(
        page,
        more_button_selectors,
        "More button",
        wait_after=1,
        dump_filename="more_menu_dom.html",
    ):
        return True

    logger.warning("Could not open the Teams More menu in the bot session.")
    return False


async def has_caption_surface(page):
    """Returns True if Teams is currently rendering live captions in this page."""
    try:
        return await page.evaluate("""() => {
            const selectors = [
                '[data-tid="closed-caption-renderer-wrapper"]',
                '[data-tid="closed-caption-text"]',
                '[role="log"]',
            ];
            return selectors.some((selector) => document.querySelector(selector));
        }""")
    except Exception as e:
        logger.debug(f"Caption surface check failed: {e}")
        return False


def get_stop_flag_path(meeting_id):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_dir, f"stop_{meeting_id}.flag")


def is_stop_requested(meeting_id):
    return os.path.exists(get_stop_flag_path(meeting_id))


async def try_enable_live_captions(page):
    """
    Opens Teams' More menu and enables live captions in the bot's own session.
    This is required for headless runs because captions are rendered per client.
    """
    if await has_caption_surface(page):
        return True

    logger.info("No live caption DOM detected yet. Trying to enable captions in the bot session...")

    if not await open_more_menu(page):
        return False

    caption_selectors = [
        "#closed-captions-button",
        "[role='menuitem'][id='closed-captions-button']",
        "[title='Show live captions']",
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
                logger.info(f"Clicked captions menu item with selector: {selector}")
                await asyncio.sleep(2)
                return await has_caption_surface(page)
        except Exception as e:
            logger.debug(f"Failed clicking captions menu item with selector {selector}: {e}")

    logger.warning("Could not find a visible Captions menu item in the Teams More menu.")
    return False


async def is_chat_compose_visible(page):
    compose_selectors = [
        "div[data-tid='ckeditor'][contenteditable='true']",
        "[data-tid='chat-pane-compose-message-footer']",
        "button[data-tid='newMessageCommands-send']",
    ]

    for selector in compose_selectors:
        try:
            locator = page.locator(selector)
            count = await locator.count()
            for idx in range(count):
                candidate = locator.nth(idx)
                if await candidate.is_visible(timeout=500):
                    return True
        except Exception as e:
            logger.debug(f"Chat compose visibility check failed for {selector}: {e}")

    return False


async def ensure_chat_panel_open(page):
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
                logger.info(f"Clicked chat button with selector: {selector}")
                await asyncio.sleep(2)
                if await is_chat_compose_visible(page):
                    return True
        except Exception as e:
            logger.debug(f"Failed opening chat panel with selector {selector}: {e}")

    logger.warning("Could not open the Teams chat panel.")
    return False


async def send_chat_message(page, message):
    """Send a one-time chat message to the Teams meeting chat."""
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
        except Exception as e:
            logger.debug(f"Failed locating chat editor with selector {selector}: {e}")

    if editor is None:
        logger.warning("Could not find a visible Teams chat editor.")
        return False

    try:
        await editor.click(timeout=5000)
        await editor.fill(message)
    except Exception as e:
        logger.warning(f"Could not fill Teams chat editor directly: {e}")
        try:
            await editor.click(timeout=5000)
            await page.keyboard.press("Meta+A")
            await page.keyboard.type(message)
        except Exception as keyboard_error:
            logger.error(f"Could not type the Teams chat announcement: {keyboard_error}")
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
        except Exception as e:
            logger.debug(f"Failed clicking send button with selector {selector}: {e}")

    try:
        await editor.press("Meta+Enter")
        logger.info("Message sent to Teams chat via keyboard shortcut: %s", message)
        return True
    except Exception as e:
        logger.error(f"Could not send the Teams chat announcement: {e}")
        return False


async def send_chat_notice(page):
    """Send the initial bot notice to the Teams meeting chat."""
    return await send_chat_message(page, CHAT_NOTICE_MESSAGE)


def normalize_chat_command_text(text):
    normalized = (
        (text or "")
        .casefold()
    )
    return " ".join(normalized.split())


def is_exit_command_message(text):
    return normalize_chat_command_text(text) == CHAT_EXIT_COMMAND


async def get_chat_messages(page):
    """Read currently visible Teams meeting chat messages."""
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
    except Exception as e:
        logger.debug(f"Could not read Teams chat messages: {e}")
        return []


async def detect_exit_command(page, known_message_ids):
    """Detect a new chat command that instructs the bot to leave."""
    messages = await get_chat_messages(page)
    if not messages:
        return False, known_message_ids

    updated_ids = set(known_message_ids)
    for message in messages:
        mid = message.get("mid", "")
        author = (message.get("author") or "").strip()
        text = (message.get("text") or "").strip()

        if not mid:
            continue
        if mid in updated_ids:
            continue

        updated_ids.add(mid)

        if not text or author == "Transcription Bot":
            continue

        if is_exit_command_message(text):
            logger.info(
                "Chat exit command detected from '%s': %s",
                author or "Unknown",
                text,
            )
            return True, updated_ids

    return False, updated_ids


async def leave_meeting_via_ui(page):
    """Try to leave the Teams meeting before closing the browser."""
    leave_selectors = [
        "button#hangup-button",
        "button[title='Leave']",
        "button[aria-label='Leave']",
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
                logger.info(f"Clicked leave button with selector: {selector}")
                await asyncio.sleep(2)
                return True
        except Exception as e:
            logger.debug(f"Failed clicking leave button with selector {selector}: {e}")

    logger.warning("Could not find a visible Leave button before shutdown.")
    return False


def get_live_meeting_screenshot_path(meeting_id):
    """Return the per-user, per-meeting asset path for live screenshots."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.abspath(os.path.join(current_dir, '..', 'app', 'reflex.db'))
    screenshot_dir = os.path.abspath(
        os.path.join(current_dir, '..', 'app', 'assets', 'live_meeting_frames')
    )
    os.makedirs(screenshot_dir, exist_ok=True)

    user_id = "unknown"
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT user_id FROM meeting WHERE id = ?",
            (meeting_id,),
        ).fetchone()
        if row and row[0] is not None:
            user_id = str(row[0])
    except Exception as e:
        logger.debug(f"Could not resolve meeting owner for screenshot path: {e}")
    finally:
        conn.close()

    return os.path.join(screenshot_dir, f"user_{user_id}_meeting_{meeting_id}.png")


def delete_live_meeting_screenshot(screenshot_path):
    """Delete a meeting-scoped screenshot file if it exists."""
    if not screenshot_path:
        return
    try:
        os.remove(screenshot_path)
        logger.info("Deleted live meeting screenshot: %s", screenshot_path)
    except FileNotFoundError:
        return
    except Exception as e:
        logger.debug(f"Could not delete live meeting screenshot {screenshot_path}: {e}")


async def take_periodic_screenshot(page, stop_event, screenshot_path):
    """Takes a screenshot every 10 seconds until stop_event is set."""
    while not stop_event.is_set():
        try:
            await page.screenshot(path=screenshot_path)
            logger.info(f"Periodic screenshot updated: {screenshot_path}")
        except Exception as e:
            logger.error(f"Failed to take periodic screenshot: {e}")
        await asyncio.sleep(10)

async def dump_dom(page, filename="debug_dom.html"):
    """Dumps the current page HTML to a file for debugging."""
    try:
        content = await page.content()
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        logger.info(f"DOM dumped to {path}")
    except Exception as e:
        logger.error(f"Failed to dump DOM: {e}")

# Database helper for the bot process
def save_transcript(meeting_id, speaker, text):
    # Find the database relative to this script
    current_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.abspath(os.path.join(current_dir, '..', 'app', 'reflex.db'))
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO transcript (meeting_id, speaker, text, timestamp) VALUES (?, ?, ?, datetime('now'))",
            (meeting_id, speaker, text)
        )
        conn.commit()
        logger.info(f"Transcript saved for meeting {meeting_id}")
    except Exception as e:
        logger.error(f"Failed to save transcript: {e}")
    finally:
        conn.close()

def update_meeting_status(meeting_id, status, clear_bot_pid=False):
    """Updates the status of a meeting in the database."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.abspath(os.path.join(current_dir, '..', 'app', 'reflex.db'))
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        if clear_bot_pid:
            cursor.execute(
                "UPDATE meeting SET status = ?, bot_pid = NULL WHERE id = ?",
                (status, meeting_id)
            )
        else:
            cursor.execute(
                "UPDATE meeting SET status = ? WHERE id = ?",
                (status, meeting_id)
            )
        conn.commit()
        logger.info(f"Meeting {meeting_id} status updated to {status}")
    except Exception as e:
        logger.error(f"Failed to update status: {e}")
    finally:
        conn.close()

async def run_bot(meeting_url, meeting_id):
    logger.info("Bot starting for meeting ID: %s", meeting_id)
    shutdown_requested = False
    leave_attempted = False
    meeting_screenshot_path = get_live_meeting_screenshot_path(meeting_id)

    def request_shutdown(signum, _frame):
        nonlocal shutdown_requested
        shutdown_requested = True
        logger.info(
            "Received signal %s. Preparing to leave meeting %s.",
            signum,
            meeting_id,
        )

    previous_handlers = {}
    for current_signal in (signal.SIGTERM, signal.SIGINT):
        try:
            previous_handlers[current_signal] = signal.getsignal(current_signal)
            signal.signal(current_signal, request_shutdown)
        except Exception as e:
            logger.debug(f"Could not register signal handler for {current_signal}: {e}")

    async with async_playwright() as p:
        try:
            logger.info("Launching browser...")
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                permissions=["microphone"]
            )
            page = await context.new_page()
            
            # Start periodic screenshot task
            stop_screenshots = asyncio.Event()
            screenshot_task = asyncio.create_task(
                take_periodic_screenshot(page, stop_screenshots, meeting_screenshot_path)
            )
            
            logger.info(f"Navigating to meeting URL: {meeting_url}")
            await page.goto(meeting_url)
            
            # Handle "Continue on this browser" button if it appears
            try:
                # Teams often takes a while to load this screen
                logger.info("Waiting for 'Continue on this browser' button or join screen...")
                btn = page.locator("button:has-text('Continue on this browser')")
                if await btn.is_visible(timeout=15000):
                    logger.info("Clicking 'Continue on this browser' button.")
                    await btn.click()
            except Exception as e:
                logger.debug(f"Continue button not found or not clickable (ignoring): {e}")

            # Handle Audio/Video Permission Popups
            async def handle_popups():
                try:
                    logger.info("Checking for audio/video permission popups...")
                    # These are common "blocker" buttons that prevent reaching the join/input fields
                    permission_btns = [
                        "button:has-text('Continue without audio or video')",
                        "button:has-text('Continue on this browser')",
                        "button:has-text('Allow')",
                        "button:has-text('Dismiss')",
                        "button:has-text('Got it')",
                        "button[aria-label*='Continue without']",
                        "button[data-tid*='continue-without']"
                    ]
                    for selector in permission_btns:
                        try:
                            # Use count() to check for multiple or existence without exception
                            btn = page.locator(selector)
                            count = await btn.count()
                            if count > 0:
                                for i in range(count):
                                    target = btn.nth(i)
                                    if await target.is_visible(timeout=2000):
                                        logger.info(f"Clicking permission/blocker button: {selector}")
                                        # Use force=True to click even if arguably covered
                                        await target.click(force=True, timeout=10000)
                                        await asyncio.sleep(2)
                                        return True
                        except:
                            continue
                except Exception as e:
                    logger.debug(f"Permission popup check failure: {e}")
                return False

            # Check once before starting search for input
            await handle_popups()

            # Wait for the join UI - Trying multiple common selectors
            logger.info("Waiting for join name input (up to 60s)...")
            name_input_selectors = [
                "input[placeholder='Type your name']",
                "input[data-tid='prejoin-display-name-input']",
                "input[name='displayName']",
                "input.input-field"
            ]
            
            name_input = None
            # Increase wait time for the first input to appear
            try:
                await page.wait_for_selector("input", timeout=60000)
            except Exception as e:
                logger.error(f"Page did not load any input within 60s: {e}")
                screenshot_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"timeout_input_{datetime.now().strftime('%H%M%S')}.png")
                await page.screenshot(path=screenshot_path)
                return

            for selector in name_input_selectors:
                try:
                    name_input = page.locator(selector)
                    if await name_input.is_visible(timeout=2000):
                        logger.info(f"Found name input with selector: {selector}")
                        break
                except:
                    continue
            
            if not name_input or not await name_input.is_visible(timeout=500):
                logger.warning("Preferred selectors failed. Using first visible input.")
                name_input = page.locator("input").first
            
            await name_input.fill("Transcription Bot")
            logger.info("Name filled.")
            
            # Re-check for popups after filling name, as they sometimes pop up then
            await handle_popups()
            
            # Click Join now
            logger.info("Waiting for 'Join now' button (up to 30s)...")
            join_buttons = [
                "button:has-text('Join now')",
                "button[data-tid='prejoin-join-button']",
                "button.primary-button",
                "button[type='button']:has-text('Join')"
            ]
            
            joined = False
            # Wait for any of the join buttons to appear
            for _ in range(45): # 45 seconds poll
                # Continuous check for popups during join wait
                await handle_popups()
                
                for btn_selector in join_buttons:
                    try:
                        btn = page.locator(btn_selector)
                        if await btn.is_visible(timeout=500):
                            # Try to click it
                            await btn.click(timeout=2000)
                            joined = True
                            logger.info(f"Clicked join button with selector: {btn_selector}")
                            break
                    except Exception as e:
                        # If click is intercepted by a popup, handle_popups will catch it in the next iteration
                        continue
                if joined:
                    break
                await asyncio.sleep(1)

            if not joined:
                logger.error("Could not find a Join button after 30s. Taking screenshot.")
                screenshot_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"timeout_join_{datetime.now().strftime('%H%M%S')}.png")
                await page.screenshot(path=screenshot_path)
                return
            
            logger.info("Joined successfully! Monitoring for captions...")
            
            # Update status to active in database
            update_meeting_status(meeting_id, "active")
            
            # Wait for meeting UI to fully load
            await asyncio.sleep(15)
            await page.screenshot(path=meeting_screenshot_path)
            logger.info("Post-join screenshot taken. Stopping periodic shots.")
            stop_screenshots.set()
            
            logger.info("Enabling live captions in the bot session.")

            # Initial DOM dump
            await dump_dom(page, "initial_join_dom.html")

            initial_caption_enabled = await try_enable_live_captions(page)
            if initial_caption_enabled:
                logger.info("Live captions enabled immediately after join.")
            else:
                logger.warning("Initial live caption enable attempt did not produce a caption DOM.")
            
            # ---- CAPTION MONITORING ----
            logger.info("Starting caption monitoring...")
            
            # Track saved captions to avoid duplicates
            saved_captions = set()
            first_transcript_preview_captured = False
            # Debounce: track the "current" caption being spoken per speaker
            # Key = speaker index, Value = {speaker, text, last_updated}
            pending_captions = {}
            DEBOUNCE_SECONDS = 1.5  # Wait 1.5s of no change before saving
            poll_count = 0
            caption_discovery_done = False
            caption_enable_attempts = 0
            chat_notice_sent = False
            chat_notice_attempts = 0
            known_chat_message_ids = set()

            if chat_notice_attempts < 3:
                chat_notice_attempts += 1
                chat_notice_sent = await send_chat_notice(page)

            for message in await get_chat_messages(page):
                mid = message.get("mid")
                if mid:
                    known_chat_message_ids.add(mid)
            
            while True:
                try:
                    if shutdown_requested or is_stop_requested(meeting_id):
                        logger.info(
                            "Stop requested for meeting %s. Closing bot session.",
                            meeting_id,
                        )
                        if not leave_attempted:
                            leave_attempted = True
                            await leave_meeting_via_ui(page)
                        break

                    poll_count += 1
                    now = datetime.now().timestamp()
                    
                    # Every 60 seconds, dump DOM for debugging
                    if poll_count % 120 == 0:
                        await dump_dom(page, "monitoring_debug_dom.html")
                    
                    # Discovery scan — first 5 minutes, every 30s
                    if not caption_discovery_done and poll_count % 60 == 1:
                        logger.info("Running caption element discovery scan...")
                        discovery = await page.evaluate("""() => {
                            const results = {};
                            const patterns = [
                                { name: 'data-tid caption', sel: '[data-tid*="caption"]' },
                                { name: 'class caption', sel: '[class*="caption"]' },
                                { name: 'role log', sel: '[role="log"]' },
                                { name: 'data-tid closed-captions', sel: '[data-tid*="closed-captions"]' },
                            ];
                            for (const p of patterns) {
                                const els = document.querySelectorAll(p.sel);
                                if (els.length > 0) {
                                    results[p.name] = els.length + ' elements, first: ' + 
                                        els[0].tagName + ' | text: ' + (els[0].innerText || '').substring(0, 100) +
                                        ' | html: ' + els[0].outerHTML.substring(0, 200);
                                }
                            }
                            return results;
                        }""")
                        if discovery:
                            for pattern, info in discovery.items():
                                logger.info(f"DISCOVERY [{pattern}]: {info}")
                        else:
                            logger.info("Discovery scan: No caption-related elements found yet.")
                        if poll_count > 600:
                            caption_discovery_done = True
                    
                    # ---- Main caption reading: read all visible caption items ----
                    caption_items = await page.evaluate("""() => {
                        const results = [];
                        const wrapper = document.querySelector('[data-tid="closed-caption-renderer-wrapper"]');

                        if (wrapper) {
                            const captionTexts = wrapper.querySelectorAll('[data-tid="closed-caption-text"]');
                            for (const textEl of captionTexts) {
                                const text = textEl.innerText?.trim();
                                if (!text) continue;
                                
                                let speaker = 'Unknown';
                                
                                let node = textEl.parentElement;
                                for (let i = 0; i < 8 && node && node !== wrapper; i++) {
                                    const fullText = node.innerText?.trim() || '';
                                    if (fullText.length > text.length) {
                                        const lines = fullText.split('\\n').map(l => l.trim()).filter(l => l.length > 0);
                                        const textIdx = lines.findIndex(l => l === text || text.startsWith(l) || l.startsWith(text));
                                        if (textIdx > 0) {
                                            const possibleSpeaker = lines[textIdx - 1];
                                            if (possibleSpeaker && possibleSpeaker.length < 50 && possibleSpeaker !== text) {
                                                speaker = possibleSpeaker;
                                                break;
                                            }
                                        }
                                    }
                                    node = node.parentElement;
                                }
                                
                                results.push({ speaker, text, idx: results.length });
                            }
                            return results;
                        }

                        const logs = document.querySelectorAll('[role="log"]');
                        for (const log of logs) {
                            const lines = (log.innerText || '')
                                .split('\\n')
                                .map(line => line.trim())
                                .filter(Boolean);
                            if (lines.length < 2) continue;

                            for (let i = 0; i < lines.length - 1; i += 2) {
                                const speaker = lines[i];
                                const text = lines[i + 1];
                                if (!speaker || !text) continue;
                                results.push({ speaker, text, idx: results.length });
                            }
                        }
                        
                        return results;
                    }""")

                    # Live captions are local to the bot's own browser tab.
                    # In headless mode they must be enabled from this session, not the user's.
                    if not caption_items and caption_enable_attempts < 3 and poll_count >= 10 and poll_count % 60 == 10:
                        caption_enable_attempts += 1
                        enabled = await try_enable_live_captions(page)
                        if enabled:
                            logger.info("Live captions enabled in the bot session.")
                        else:
                            logger.warning("Live captions still not visible after enable attempt.")
                    
                    if (
                        not chat_notice_sent
                        and chat_notice_attempts < 3
                        and poll_count >= 20
                        and poll_count % 60 == 20
                    ):
                        chat_notice_attempts += 1
                        chat_notice_sent = await send_chat_notice(page)

                    if poll_count % 4 == 0:
                        exit_requested, known_chat_message_ids = await detect_exit_command(
                            page,
                            known_chat_message_ids,
                        )
                        if exit_requested:
                            logger.info(
                                "Leaving meeting %s because a user sent the '%s' chat command.",
                                meeting_id,
                                CHAT_EXIT_COMMAND,
                            )
                            await send_chat_message(page, CHAT_EXIT_ACK_MESSAGE)
                            if not leave_attempted:
                                leave_attempted = True
                                await leave_meeting_via_ui(page)
                            break

                    if caption_items:
                        for item in caption_items:
                            idx = item.get('idx', 0)
                            text = item.get('text', '').strip()
                            speaker = item.get('speaker', 'Unknown').strip()
                            
                            if not text or len(text) < 2:
                                continue
                            
                            # Skip system messages
                            if text in ("Captions are turned on.", "Captions are turned off."):
                                continue
                            
                            # Update pending caption for this index
                            key = f"{idx}"
                            prev = pending_captions.get(key)
                            
                            if prev is None or prev['text'] != text:
                                # Text changed — update pending
                                pending_captions[key] = {
                                    'speaker': speaker,
                                    'text': text,
                                    'last_updated': now,
                                }
                            # If text is same, just leave it (debounce timer continues)
                    
                    # ---- Save debounced captions ----
                    keys_to_remove = []
                    for key, cap in pending_captions.items():
                        elapsed = now - cap['last_updated']
                        if elapsed >= DEBOUNCE_SECONDS:
                            text = cap['text']
                            speaker = cap['speaker']
                            # Create a dedup key
                            dedup_key = f"{speaker}:{text}"
                            if dedup_key not in saved_captions:
                                saved_captions.add(dedup_key)
                                logger.info(f"CAPTION SAVED - [{speaker}]: {text}")
                                save_transcript(meeting_id, speaker, text)
                                if not first_transcript_preview_captured:
                                    try:
                                        await page.screenshot(path=meeting_screenshot_path)
                                        first_transcript_preview_captured = True
                                        logger.info(
                                            "Captured live meeting preview after first transcript."
                                        )
                                    except Exception as e:
                                        logger.warning(
                                            "Failed to capture first transcript preview: %s",
                                            e,
                                        )
                            keys_to_remove.append(key)
                    
                    for key in keys_to_remove:
                        del pending_captions[key]
                    
                    # Prevent memory leak
                    if len(saved_captions) > 2000:
                        saved_captions = set(list(saved_captions)[-1000:])
                
                except Exception as e:
                    logger.error(f"Error during caption polling: {e}", exc_info=True)
                    try:
                        await page.evaluate("1+1")
                    except Exception:
                        logger.error("Page disconnected! Meeting may have ended.")
                        try:
                            await page.screenshot(path=os.path.join(os.path.dirname(os.path.abspath(__file__)), "exit_screenshot.png"))
                        except:
                            pass
                        break
                
                await asyncio.sleep(0.5)
            
            logger.info("Caption monitoring loop ended.")
        except Exception as e:
            logger.error(f"An error occurred during bot execution: {e}", exc_info=True)
        finally:
            if 'stop_screenshots' in locals():
                stop_screenshots.set()
                await screenshot_task
            if (
                'page' in locals()
                and not page.is_closed()
                and (shutdown_requested or is_stop_requested(meeting_id))
                and not leave_attempted
            ):
                try:
                    leave_attempted = True
                    await leave_meeting_via_ui(page)
                except Exception as e:
                    logger.debug(f"Failed final leave attempt: {e}")
            if 'browser' in locals():
                logger.info("Closing browser.")
                await browser.close()
            update_meeting_status(meeting_id, "completed", clear_bot_pid=True)
            delete_live_meeting_screenshot(meeting_screenshot_path)
            stop_flag_path = get_stop_flag_path(meeting_id)
            if os.path.exists(stop_flag_path):
                try:
                    os.remove(stop_flag_path)
                except OSError as e:
                    logger.debug(f"Could not remove stop flag {stop_flag_path}: {e}")
            for current_signal, previous_handler in previous_handlers.items():
                try:
                    signal.signal(current_signal, previous_handler)
                except Exception:
                    pass

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 2:
        asyncio.run(run_bot(sys.argv[1], sys.argv[2]))
    else:
        print("Usage: python bot.py <url> <id>")
