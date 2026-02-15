"""
Telegram ↔ Claude Code Bridge (tmux edition)

Receives tasks via Telegram, types them into a live Claude Code
tmux session. The user can see Claude working on-screen in real time.

Usage:
    python telegram_bridge.py

To watch Claude work:
    tmux attach -t claude
"""

import asyncio
import logging
import os
import re
import subprocess
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ── Config ────────────────────────────────────────────────
_config_dir = Path(__file__).parent / "config"
_env_path = _config_dir / ".env"
if _env_path.exists():
    load_dotenv(_env_path)
else:
    load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
PROJECT_DIR = Path(__file__).parent.resolve()
CLAUDE_BIN = os.getenv("CLAUDE_BIN", "claude")

TMUX_SESSION = "claude"
TMUX_PANE = f"{TMUX_SESSION}:0"
STABLE_SECONDS = 12        # seconds of no output = Claude is done
MAX_TIMEOUT = 600           # 10 min max per task
TELEGRAM_MSG_LIMIT = 4096

# ── Logging ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-18s | %(levelname)-8s | %(message)s",
)
log = logging.getLogger("telegram_bridge")

# ── State ─────────────────────────────────────────────────
_task_running = False


# ── tmux helpers ──────────────────────────────────────────

def tmux_session_exists() -> bool:
    r = subprocess.run(
        ["tmux", "has-session", "-t", TMUX_SESSION],
        capture_output=True,
    )
    return r.returncode == 0


def capture_pane() -> str:
    """Capture clean text from tmux pane (no ANSI codes)."""
    r = subprocess.run(
        ["tmux", "capture-pane", "-t", TMUX_PANE, "-p", "-S", "-2000"],
        capture_output=True, text=True,
    )
    return r.stdout


def setup_tmux() -> bool:
    """Create tmux session with Claude Code if not running."""
    if tmux_session_exists():
        log.info("tmux session already exists")
        return True

    log.info("Creating tmux session with Claude Code...")

    # Create session
    subprocess.run([
        "tmux", "new-session", "-d", "-s", TMUX_SESSION,
        "-x", "200", "-y", "50",
    ])

    # Larger scrollback
    subprocess.run([
        "tmux", "set-option", "-t", TMUX_SESSION,
        "history-limit", "50000",
    ])

    # Unset CLAUDECODE env and start Claude in project dir
    startup_cmd = (
        f"cd {PROJECT_DIR} && "
        f"unset CLAUDECODE && "
        f"{CLAUDE_BIN}"
    )
    subprocess.run([
        "tmux", "send-keys", "-t", TMUX_PANE, startup_cmd, "Enter",
    ])

    # Wait for Claude to initialize
    log.info("Waiting for Claude Code to start...")
    time.sleep(8)

    pane = capture_pane()
    if pane.strip():
        log.info("Claude Code started in tmux session")
        return True
    else:
        log.error("Claude Code may not have started properly")
        return False


def send_keys(text: str) -> None:
    """Send keystrokes to tmux pane."""
    # Use send-keys -l for literal text (avoids tmux key interpretation)
    subprocess.run([
        "tmux", "send-keys", "-t", TMUX_PANE, "-l", text,
    ])
    # Press Enter
    subprocess.run([
        "tmux", "send-keys", "-t", TMUX_PANE, "Enter",
    ])


def wait_for_completion(before_snapshot: str, timeout: int = MAX_TIMEOUT) -> str:
    """Wait for Claude to finish and return new output."""
    time.sleep(3)  # initial wait for Claude to start processing

    last_capture = capture_pane()
    stable = 0
    start = time.time()

    while stable < STABLE_SECONDS and (time.time() - start) < timeout:
        time.sleep(1)
        current = capture_pane()
        if current == last_capture:
            stable += 1
        else:
            stable = 0
            last_capture = current

    # Extract new content by diffing
    before_lines = before_snapshot.splitlines()
    after_lines = last_capture.splitlines()

    # Find where old content ends and new begins
    # Skip lines that match the before snapshot
    start_idx = 0
    for i, line in enumerate(after_lines):
        if i < len(before_lines) and line == before_lines[i]:
            start_idx = i + 1
        else:
            break

    new_lines = after_lines[start_idx:]
    result = "\n".join(new_lines).strip()

    # Clean up any remaining escape sequences
    result = re.sub(r'\x1b\[[0-9;]*[a-zA-Z]', '', result)
    result = re.sub(r'\x1b\][^\x07]*\x07', '', result)

    return result


# ── Authorization ─────────────────────────────────────────

def is_authorized(update: Update) -> bool:
    chat_id = str(update.effective_chat.id)
    if CHAT_ID and chat_id != CHAT_ID:
        log.warning(f"Unauthorized: chat_id={chat_id}")
        return False
    return True


def split_message(text: str, limit: int = TELEGRAM_MSG_LIMIT) -> list[str]:
    if len(text) <= limit:
        return [text]
    chunks = []
    while text:
        if len(text) <= limit:
            chunks.append(text)
            break
        split_at = text.rfind("\n", 0, limit)
        if split_at == -1:
            split_at = limit
        chunks.append(text[:split_at])
        text = text[split_at:].lstrip("\n")
    return chunks


async def send_long_message(update: Update, text: str) -> None:
    if not text.strip():
        text = "(ריק — אין פלט)"
    for chunk in split_message(text):
        await update.message.reply_text(chunk)


# ── Handlers ──────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    if not CHAT_ID:
        await update.message.reply_text(
            f"Chat ID: `{chat_id}`\n\n"
            f"הוסף ל-config/.env והפעל מחדש.",
            parse_mode="Markdown",
        )
        return
    if not is_authorized(update):
        return
    await update.message.reply_text(
        "🤖 *Claude Code Bridge פעיל (tmux)*\n\n"
        "שלח הודעה ← Claude Code מבצע ← תוצאה חוזרת\n"
        "צפה בזמן אמת: `tmux attach -t claude`\n\n"
        "/status — סטטוס\n"
        "/cancel — ביטול\n"
        "/help — עזרה",
        parse_mode="Markdown",
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_authorized(update):
        return
    if not tmux_session_exists():
        await update.message.reply_text("❌ tmux session לא פעיל")
        return
    if _task_running:
        await update.message.reply_text("⏳ יש משימה רצה כרגע...")
    else:
        await update.message.reply_text("✅ פנוי — מחכה למשימה.")


async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_authorized(update):
        return
    # Send Ctrl+C to Claude
    subprocess.run(["tmux", "send-keys", "-t", TMUX_PANE, "C-c", ""])
    await update.message.reply_text("🛑 שלחתי Ctrl+C ל-Claude.")


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_authorized(update):
        return
    await update.message.reply_text(
        "📖 *Claude Code Bridge — tmux*\n\n"
        "כל הודעה נשלחת ישירות ל-Claude Code.\n"
        "Claude עובד ב-tmux session שאתה יכול לראות:\n"
        "`tmux attach -t claude`\n\n"
        "• /status — סטטוס\n"
        "• /cancel — Ctrl+C\n"
        "• /help — עזרה",
        parse_mode="Markdown",
    )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global _task_running
    if not is_authorized(update):
        return

    task = update.message.text.strip()
    if not task:
        return

    if _task_running:
        await update.message.reply_text(
            "⏳ יש משימה רצה. חכה או שלח /cancel"
        )
        return

    if not tmux_session_exists():
        await update.message.reply_text("❌ tmux session לא פעיל. מפעיל...")
        setup_tmux()

    _task_running = True
    await update.message.reply_text(f"🔄 עובד על זה...\n📺 צפה: `tmux attach -t claude`", parse_mode="Markdown")

    log.info(f"Task: {task[:80]}...")

    loop = asyncio.get_event_loop()

    def _execute():
        global _task_running
        try:
            before = capture_pane()
            send_keys(task)
            result = wait_for_completion(before)
            return result
        finally:
            _task_running = False

    result = await loop.run_in_executor(None, _execute)

    await send_long_message(update, result)
    log.info(f"Task done, response length: {len(result)}")


# ── Main ──────────────────────────────────────────────────

def main() -> None:
    if not BOT_TOKEN:
        print("❌ TELEGRAM_BOT_TOKEN לא מוגדר ב-config/.env")
        sys.exit(1)

    log.info("Starting Telegram ↔ Claude Code bridge (tmux edition)")
    log.info(f"Project dir: {PROJECT_DIR}")

    # Ensure tmux session with Claude is running
    setup_tmux()

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    log.info("Bridge running. Send a message on Telegram!")
    log.info(f"Watch Claude: tmux attach -t {TMUX_SESSION}")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
