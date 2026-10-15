import asyncio
import html
import logging
import math
import os
import secrets
import threading
import time
from collections import OrderedDict
from logging.handlers import RotatingFileHandler
from pathlib import Path

from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import (Application, CallbackQueryHandler, CommandHandler,
                          ContextTypes, MessageHandler, filters)
from telegram.request import HTTPXRequest

from searcher import READERS, Index

# Everything is relative to this file, so it works no matter where a service
# manager starts it from.
BASE = Path(__file__).resolve().parent
load_dotenv(BASE / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[
        RotatingFileHandler(BASE / "bot.log", maxBytes=2_000_000,
                            backupCount=3, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logging.getLogger("httpx").setLevel(logging.WARNING)   # its URLs contain your bot token
log = logging.getLogger("bot")

TOKEN = os.getenv("BOT_TOKEN")
ALLOWED = {int(x) for x in os.getenv("ALLOWED_IDS", "").split(",") if x.strip()}
FOLDER = BASE / "files"
FOLDER.mkdir(exist_ok=True)
index = Index(FOLDER, BASE / "index.db")

PAGE_SIZE = 5
MAX_RESULTS = 200
MAX_STORED_SEARCHES = 50


def allowed(update: Update) -> bool:
    user = update.effective_user
    return bool(user and user.id in ALLOWED)


# ------------------------------------------------------------------- commands
HELP = (
    "Send me any word to search your files.\n"
    "• Several words → all must appear together\n"
    '• "in quotes" → exact phrase\n'
    "• Typos are forgiven when nothing matches exactly\n"
    "• Filter spreadsheet columns with column:value, e.g.\n"
    "   first:john last:smith city:accra\n"
    '   city:"cape coast" (quotes for several words)\n'
    "   Use /columns to see what columns exist.\n\n"
    "Upload a file (txt, md, log, csv, xlsx, xls, docx, pdf) to add it.\n\n"
    "/files - list stored files\n"
    "/columns - show spreadsheet columns you can filter on\n"
    "/reindex - rescan the folder"
)


async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not allowed(update):
        return
    await update.message.reply_text(HELP)


async def list_files(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not allowed(update):
        return
    names = sorted(str(p.relative_to(FOLDER)) for p in FOLDER.rglob("*") if p.is_file())
    text = "\n".join(names) or "No files yet."
    for i in range(0, len(text), 4000):
        await update.message.reply_text(text[i:i + 4000])


async def show_columns(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not allowed(update):
        return
    cols = await asyncio.to_thread(index.columns)
    if not cols:
        await update.message.reply_text("No spreadsheets with columns indexed yet.")
        return
    text = "Filter with the first word of a column, e.g. first:john\n\n" + "\n".join(
        f"📄 {f}\n   " + ", ".join(h) for f, h in cols.items())
    for i in range(0, len(text), 4000):
        await update.message.reply_text(text[i:i + 4000])


async def reindex(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not allowed(update):
        return
    msg = await update.message.reply_text("Reindexing…")
    changed, removed = await asyncio.to_thread(index.sync)
    await msg.edit_text(f"Done. {changed} file(s) indexed, {removed} removed.")


# --------------------------------------------------------------------- search
def render(sid: str, s: dict, page: int):
    results = s["results"]
    pages = max(1, math.ceil(len(results) / PAGE_SIZE))
    page = min(max(page, 0), pages - 1)
    chunk = results[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]

    count = f"{len(results)}+" if len(results) >= MAX_RESULTS else str(len(results))
    head = f"🔎 <b>{html.escape(s['query'])}</b> — {count} match(es)"
    if s["fuzzy"]:
        head += "\n<i>No exact matches — showing close matches.</i>"
    body = "\n\n".join(
        f"📄 <b>{html.escape(f)}</b>\n📍 {html.escape(w)}\n{snip}"
        for f, w, snip in chunk)
    text = f"{head}\n\n{body}\n\nPage {page + 1}/{pages}"

    markup = None
    if pages > 1:
        row = []
        if page > 0:
            row.append(InlineKeyboardButton("◀ Prev", callback_data=f"pg:{sid}:{page - 1}"))
        row.append(InlineKeyboardButton(f"{page + 1}/{pages}", callback_data="noop"))
        if page < pages - 1:
            row.append(InlineKeyboardButton("Next ▶", callback_data=f"pg:{sid}:{page + 1}"))
        markup = InlineKeyboardMarkup([row])
    return text, markup


async def handle_search(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not allowed(update):
        return
    query = update.message.text.strip()
    status = await update.message.reply_text("🔎 Searching…")
    t0 = time.perf_counter()
    results, fuzzy = await asyncio.to_thread(index.search, query, MAX_RESULTS)
    log.info("Search %r: %d results, fuzzy=%s, %.0f ms",
             query, len(results), fuzzy, (time.perf_counter() - t0) * 1000)
    if not results:
        await status.edit_text("No matches found.")
        return

    store = ctx.bot_data.setdefault("searches", OrderedDict())
    sid = secrets.token_hex(4)
    store[sid] = {"user": update.effective_user.id, "query": query,
                  "results": results, "fuzzy": fuzzy}
    while len(store) > MAX_STORED_SEARCHES:
        store.popitem(last=False)

    text, markup = render(sid, store[sid], 0)
    await status.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)


async def on_page(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q.from_user.id not in ALLOWED or q.data == "noop":
        await q.answer()
        return
    _, sid, page = q.data.split(":")
    s = ctx.bot_data.get("searches", {}).get(sid)
    if not s or s["user"] != q.from_user.id:
        await q.answer("This search has expired — please search again.", show_alert=True)
        return
    text, markup = render(sid, s, int(page))
    await q.answer()
    try:
        await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)
    except BadRequest as e:
        if "not modified" not in str(e).lower():
            raise


# --------------------------------------------------------------------- upload
async def handle_upload(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not allowed(update):
        return
    doc = update.message.document
    name = Path(doc.file_name or "").name
    if not name or Path(name).suffix.lower() not in READERS:
        await update.message.reply_text(
            "Unsupported file type. Supported: " + ", ".join(sorted(READERS)))
        return
    status = await update.message.reply_text(f"Downloading {name}…")
    try:
        tg_file = await doc.get_file()
        dest = FOLDER / name
        await tg_file.download_to_drive(dest)
    except Exception as e:
        log.exception("Download failed for %s", name)
        await status.edit_text(f"Couldn't download {name}: {e}")
        return
    await status.edit_text(f"Saved {name}, indexing…")
    n = await asyncio.to_thread(index.index_file, dest)
    if n is None:
        await status.edit_text(f"Saved {name}, but I couldn't read it (corrupt or password-protected?).")
    elif n == 0:
        await status.edit_text(f"Saved {name}, but found no text in it (scanned PDF/images?).")
    else:
        await status.edit_text(f"Saved and indexed {name} ✅ ({n} entries)")


# ------------------------------------------------------------------- lifecycle
async def post_init(app: Application):
    # Index in the background so the bot answers immediately after a restart.
    # daemon thread: Ctrl+C / stopping the task really exits instead of waiting for it
    threading.Thread(target=index.sync, name="sync", daemon=True).start()


async def on_error(update, ctx: ContextTypes.DEFAULT_TYPE):
    log.error("Unhandled error", exc_info=ctx.error)


def main():
    if not TOKEN:
        raise SystemExit("BOT_TOKEN is not set (put it in .env)")
    if not ALLOWED:
        log.warning("ALLOWED_IDS is empty - the bot will ignore everyone")
    request = HTTPXRequest(
        connect_timeout=15, read_timeout=30, write_timeout=30, pool_timeout=5)
    app = (Application.builder().token(TOKEN).request(request)
           .post_init(post_init).build())
    app.add_handler(CommandHandler(["start", "help"], start))
    app.add_handler(CommandHandler("files", list_files))
    app.add_handler(CommandHandler("columns", show_columns))
    app.add_handler(CommandHandler("reindex", reindex))
    app.add_handler(CallbackQueryHandler(on_page, pattern=r"^(pg:|noop)"))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_upload))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_search))
    app.add_error_handler(on_error)
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())
    try:
        app.run_polling(bootstrap_retries=-1)   # keep retrying if the network is down at boot
    except KeyboardInterrupt:
        log.info("Bot interrupted; exiting.")


if __name__ == "__main__":
    main()