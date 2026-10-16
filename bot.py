import asyncio
import logging
import os
import sys
from datetime import datetime
from pathlib import Path, PureWindowsPath
from typing import Dict, List, Optional, Tuple

import aiosqlite
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject
from aiogram.types import (
    Message,
    CallbackQuery,
    Update,
    BusinessMessagesDeleted,
    BusinessConnection,
    FSInputFile,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)

load_dotenv(Path(__file__).resolve().with_name(".env"), encoding="utf-8-sig")

BOT_TOKEN = os.getenv("BOT_TOKEN")
ALLOWED_USERS_RAW = os.getenv("ALLOWED_USERS") or os.getenv("ADMIN_IDS") or os.getenv("ADMIN_ID", "")
RAILWAY_VOLUME_ROOT = os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "").strip()
DATA_ROOT = Path(
    RAILWAY_VOLUME_ROOT or os.getenv("DATA_DIR") or Path(__file__).resolve().parent
).expanduser().resolve()
MAINTENANCE_MODE = os.getenv("MAINTENANCE_MODE", "0").strip().lower() in {"1", "true", "yes"}


def storage_path(variable: str, default: str) -> str:
    value = os.getenv(variable, "").strip() or default
    if os.name != "nt" and PureWindowsPath(value).is_absolute():
        raise RuntimeError(
            f"{variable}: путь Windows не подходит для Railway. "
            f"Используйте относительный путь {default} или путь внутри Volume."
        )
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = DATA_ROOT / path
    return str(path.resolve())


DB_PATH = storage_path("DB_PATH", "bot_archive.db")
MEDIA_DIR = storage_path("MEDIA_DIR", "bot_media")


def validate_storage():
    if os.getenv("RAILWAY_ENVIRONMENT_ID") and not RAILWAY_VOLUME_ROOT:
        raise RuntimeError("Подключите Volume к сервису Railway с Mount Path /data.")
    if RAILWAY_VOLUME_ROOT:
        mount = Path(RAILWAY_VOLUME_ROOT).resolve()
        for label, value in (("DB_PATH", DB_PATH), ("MEDIA_DIR", MEDIA_DIR)):
            if not Path(value).resolve().is_relative_to(mount):
                raise RuntimeError(f"{label} должен находиться внутри подключённого Volume.")
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    Path(MEDIA_DIR).mkdir(parents=True, exist_ok=True)


def resolve_media_path(value: Optional[str]) -> Optional[str]:
    """Resolve old Windows archive paths after copying bot_media to the Volume."""
    if not value or Path(value).is_file():
        return value
    normalized = value.replace(chr(92), "/")
    if "/" in normalized:
        candidate = Path(MEDIA_DIR) / normalized.rsplit("/", 1)[-1]
        if candidate.is_file():
            return str(candidate)
    return value  # Telegram file_id or a file that has not been transferred.


if not BOT_TOKEN and not MAINTENANCE_MODE:
    print("Ошибка: задайте BOT_TOKEN в Railway Variables или в локальном файле .env")
    sys.exit(1)

ALLOWED_USERS: List[int] = [
    int(uid.strip())
    for uid in ALLOWED_USERS_RAW.split(",")
    if uid.strip().isdigit()
]

CONNECTION_CACHE: Dict[str, int] = {}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)

router = Router()

# ================= БАЗА ДАННЫХ =================

async def init_db():
    validate_storage()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                owner_user_id INTEGER DEFAULT 0,
                sender_id INTEGER DEFAULT 0,
                sender_name TEXT DEFAULT '',
                user_id INTEGER DEFAULT 0,
                user_name TEXT DEFAULT '',
                chat_title TEXT NOT NULL,
                created_at TEXT NOT NULL,
                is_deleted INTEGER DEFAULT 0,
                deleted_at TEXT DEFAULT NULL,
                media_type TEXT DEFAULT NULL,
                media_path TEXT DEFAULT NULL,
                business_connection_id TEXT DEFAULT NULL,
                UNIQUE(chat_id, message_id)
            );
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS message_versions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_ref_id INTEGER NOT NULL,
                version_num INTEGER NOT NULL,
                text TEXT NOT NULL,
                saved_at TEXT NOT NULL,
                FOREIGN KEY(message_ref_id) REFERENCES messages(id) ON DELETE CASCADE
            );
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS business_connections (
                connection_id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                user_name TEXT NOT NULL
            );
        """)
        await db.commit()

        async with db.execute("PRAGMA table_info(messages)") as cursor:
            existing_cols = [row[1] for row in await cursor.fetchall()]

        for col_name, col_def in [
            ("owner_user_id", "INTEGER DEFAULT 0"),
            ("sender_id", "INTEGER DEFAULT 0"),
            ("sender_name", "TEXT DEFAULT ''"),
            ("user_id", "INTEGER DEFAULT 0"),
            ("user_name", "TEXT DEFAULT ''"),
            ("deleted_at", "TEXT DEFAULT NULL"),
            ("media_type", "TEXT DEFAULT NULL"),
            ("media_path", "TEXT DEFAULT NULL"),
            ("business_connection_id", "TEXT DEFAULT NULL"),
        ]:
            if col_name not in existing_cols:
                try:
                    await db.execute(f"ALTER TABLE messages ADD COLUMN {col_name} {col_def};")
                    await db.commit()
                except Exception:
                    pass

        async with db.execute("SELECT connection_id, user_id FROM business_connections") as cursor:
            rows = await cursor.fetchall()
            for conn_id, u_id in rows:
                CONNECTION_CACHE[conn_id] = u_id

async def save_business_connection(conn_id: str, user_id: int, user_name: str):
    CONNECTION_CACHE[conn_id] = user_id
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO business_connections (connection_id, user_id, user_name) VALUES (?, ?, ?)",
            (conn_id, user_id, user_name)
        )
        await db.commit()
        logger.info(f"Бизнес-подключение {conn_id} привязано к аккаунту: {user_name} ({user_id})")

async def get_connection_owner(conn_id: Optional[str], bot: Optional[Bot] = None) -> Optional[int]:
    if not conn_id:
        return None

    if conn_id in CONNECTION_CACHE:
        return CONNECTION_CACHE[conn_id]

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT user_id FROM business_connections WHERE connection_id = ?",
            (conn_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                CONNECTION_CACHE[conn_id] = row[0]
                return row[0]

    if bot:
        try:
            conn = await bot.get_business_connection(business_connection_id=conn_id)
            if conn and conn.user:
                u_name = conn.user.full_name or "Пользователь"
                await save_business_connection(conn.id, conn.user.id, u_name)
                return conn.user.id
        except Exception as e:
            logger.error(f"Не удалось получить бизнес-подключение через API: {e}")

    return None

async def save_new_message(
    owner_user_id: int,
    chat_id: int,
    message_id: int,
    sender_id: int,
    sender_name: str,
    chat_title: str,
    text: str,
    media_type: Optional[str] = None,
    media_path: Optional[str] = None,
    business_connection_id: Optional[str] = None
):
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            cursor = await db.execute(
                """
                INSERT OR REPLACE INTO messages (
                    owner_user_id, chat_id, message_id, sender_id, sender_name, user_id, user_name,
                    chat_title, created_at, is_deleted, media_type, media_path, business_connection_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
                """,
                (owner_user_id, chat_id, message_id, sender_id, sender_name, sender_id, sender_name, chat_title, now_str, media_type, media_path, business_connection_id)
            )
            msg_ref_id = cursor.lastrowid
            await db.execute(
                "INSERT INTO message_versions (message_ref_id, version_num, text, saved_at) VALUES (?, 1, ?, ?)",
                (msg_ref_id, text, now_str)
            )
            await db.commit()
            logger.info(f"✅ Успешно сохранено сообщение #{message_id} из '{chat_title}' (автор: {sender_name}, тип: {media_type or 'текст'})")
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения сообщения #{message_id}: {e}")

async def save_edited_message(chat_id: int, message_id: int, new_text: str):
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT id FROM messages WHERE chat_id = ? AND message_id = ?", (chat_id, message_id)) as cursor:
            row = await cursor.fetchone()
            if not row:
                return
            msg_ref_id = row[0]

        async with db.execute("SELECT MAX(version_num) FROM message_versions WHERE message_ref_id = ?", (msg_ref_id,)) as cursor:
            v_row = await cursor.fetchone()
            max_v = v_row[0] if v_row and v_row[0] is not None else 1
            next_v = max_v + 1

        await db.execute(
            "INSERT INTO message_versions (message_ref_id, version_num, text, saved_at) VALUES (?, ?, ?, ?)",
            (msg_ref_id, next_v, new_text, now_str)
        )
        await db.commit()
        logger.info(f"✅ Сохранена редакция #{next_v} для сообщения #{message_id}")

async def mark_and_get_deleted_messages(chat_id: int, message_ids: List[int], owner_user_id: int) -> List[dict]:
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    results = []
    async with aiosqlite.connect(DB_PATH) as db:
        for mid in message_ids:
            async with db.execute(
                """
                SELECT id, owner_user_id, COALESCE(NULLIF(sender_name, ''), NULLIF(user_name, ''), 'Неизвестный'),
                       chat_title, created_at, media_type, media_path
                FROM messages WHERE chat_id = ? AND message_id = ?
                """,
                (chat_id, mid)
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    results.append({"message_id": mid, "found": False, "owner_user_id": owner_user_id})
                    continue

                msg_ref_id, db_owner, sender_name, chat_title, created_at, media_type, media_path = row
                actual_owner = db_owner if db_owner and db_owner > 0 else owner_user_id

                await db.execute(
                    "UPDATE messages SET is_deleted = 1, deleted_at = ?, owner_user_id = ? WHERE id = ?",
                    (now_str, actual_owner, msg_ref_id)
                )

                async with db.execute(
                    "SELECT text FROM message_versions WHERE message_ref_id = ? ORDER BY version_num DESC LIMIT 1",
                    (msg_ref_id,)
                ) as v_cur:
                    v_row = await v_cur.fetchone()
                    latest_text = v_row[0] if v_row else "—"

                results.append({
                    "message_id": mid,
                    "found": True,
                    "owner_user_id": actual_owner,
                    "sender_name": sender_name,
                    "chat_title": chat_title,
                    "created_at": created_at,
                    "deleted_at": now_str,
                    "media_type": media_type,
                    "media_path": resolve_media_path(media_path),
                    "latest_text": latest_text
                })
        await db.commit()
    return results

# ================= СПИСКИ И ФИЛЬТРЫ ДЛЯ АРХИВА =================

async def get_deleted_chats_filter(owner_user_id: int) -> List[Tuple[int, str, int]]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """
            SELECT chat_id, chat_title, COUNT(*) as cnt
            FROM messages
            WHERE (owner_user_id = ? OR owner_user_id = 0) AND is_deleted = 1
            GROUP BY chat_id
            ORDER BY MAX(id) DESC
            """,
            (owner_user_id,)
        ) as cursor:
            return await cursor.fetchall()

async def get_deleted_messages_by_chat(owner_user_id: int, target_chat: str, limit: int = 100) -> List[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        if target_chat == "all":
            query = """
                SELECT m.id, m.chat_title, COALESCE(NULLIF(m.sender_name, ''), NULLIF(m.user_name, ''), 'Неизвестный'),
                       m.created_at, m.deleted_at, m.media_type, m.media_path,
                       (SELECT text FROM message_versions WHERE message_ref_id = m.id ORDER BY version_num DESC LIMIT 1) as latest_text
                FROM messages m
                WHERE (m.owner_user_id = ? OR m.owner_user_id = 0) AND m.is_deleted = 1
                ORDER BY m.id DESC LIMIT ?
            """
            params = (owner_user_id, limit)
        else:
            chat_id = int(target_chat)
            query = """
                SELECT m.id, m.chat_title, COALESCE(NULLIF(m.sender_name, ''), NULLIF(m.user_name, ''), 'Неизвестный'),
                       m.created_at, m.deleted_at, m.media_type, m.media_path,
                       (SELECT text FROM message_versions WHERE message_ref_id = m.id ORDER BY version_num DESC LIMIT 1) as latest_text
                FROM messages m
                WHERE (m.owner_user_id = ? OR m.owner_user_id = 0) AND m.is_deleted = 1 AND m.chat_id = ?
                ORDER BY m.id DESC LIMIT ?
            """
            params = (owner_user_id, chat_id, limit)

        async with db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            return [
                {
                    "id": r[0], "chat_title": r[1], "sender_name": r[2], "created_at": r[3],
                    "deleted_at": r[4] or "Недавно", "media_type": r[5], "media_path": resolve_media_path(r[6]), "text": r[7] or "—"
                }
                for r in rows
            ]

async def get_edited_messages_list(owner_user_id: int, limit: int = 50) -> List[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """
            SELECT m.id, m.chat_title, COALESCE(NULLIF(m.sender_name, ''), NULLIF(m.user_name, ''), 'Неизвестный'),
                   m.created_at, COUNT(v.id) as v_count
            FROM messages m
            JOIN message_versions v ON m.id = v.message_ref_id
            WHERE (m.owner_user_id = ? OR m.owner_user_id = 0)
            GROUP BY m.id HAVING v_count > 1
            ORDER BY m.id DESC LIMIT ?
            """,
            (owner_user_id, limit)
        ) as cursor:
            rows = await cursor.fetchall()
            res = []
            for r in rows:
                msg_ref_id = r[0]
                async with db.execute(
                    "SELECT version_num, text, saved_at FROM message_versions WHERE message_ref_id = ? ORDER BY version_num ASC",
                    (msg_ref_id,)
                ) as v_cur:
                    versions = await v_cur.fetchall()
                res.append({
                    "id": r[0], "chat_title": r[1], "sender_name": r[2], "created_at": r[3],
                    "versions": [{"version": v[0], "text": v[1], "saved_at": v[2]} for v in versions]
                })
            return res

# ================= ОБРАБОТКА МЕДИА И ИНФО =================

async def extract_and_download_media(message: Message, bot: Bot) -> Tuple[str, Optional[str], Optional[str]]:
    caption_or_text = message.text or message.caption or ""
    chat_id = message.chat.id
    msg_id = message.message_id

    if message.photo:
        photo = message.photo[-1]
        local_path = os.path.join(MEDIA_DIR, f"{chat_id}_{msg_id}.jpg")
        try:
            await bot.download(photo, destination=local_path)
            media_path = local_path
        except Exception:
            media_path = photo.file_id
        return caption_or_text or "[Фотография]", "photo", media_path

    if message.voice:
        local_path = os.path.join(MEDIA_DIR, f"{chat_id}_{msg_id}.ogg")
        try:
            await bot.download(message.voice, destination=local_path)
            media_path = local_path
        except Exception:
            media_path = message.voice.file_id
        return caption_or_text or "[Голосовое сообщение]", "voice", media_path

    if message.video_note:
        local_path = os.path.join(MEDIA_DIR, f"{chat_id}_{msg_id}.mp4")
        try:
            await bot.download(message.video_note, destination=local_path)
            media_path = local_path
        except Exception:
            media_path = message.video_note.file_id
        return caption_or_text or "[Видеосообщение (кружочек)]", "video_note", media_path

    if message.video:
        return caption_or_text or "[Видео]", "video", message.video.file_id

    if message.document:
        doc_name = message.document.file_name or "файл"
        return caption_or_text or f"[Документ: {doc_name}]", "document", message.document.file_id

    return caption_or_text, None, None

def extract_sender_info(message: Message) -> Tuple[int, int, int, str, str]:
    chat_id = message.chat.id
    message_id = message.message_id
    user_id = message.from_user.id if message.from_user else 0
    user_name = message.from_user.full_name if message.from_user else "Неизвестный"
    if message.from_user and message.from_user.username:
        user_name += f" (@{message.from_user.username})"

    chat_title = message.chat.title or message.chat.full_name or ""
    if message.chat.username:
        chat_title += f" (@{message.chat.username})"
    if not chat_title.strip():
        chat_title = f"Диалог #{chat_id}"

    return chat_id, message_id, user_id, user_name, chat_title

# ================= КЛАВИАТУРЫ =================

def get_main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🗑 Удалённые сообщения"), KeyboardButton(text="✏️ Изменённые сообщения")],
            [KeyboardButton(text="📊 Статистика архива"), KeyboardButton(text="ℹ️ Помощь")]
        ],
        resize_keyboard=True
    )

def build_chats_filter_keyboard(chats: List[Tuple[int, str, int]], total_count: int) -> InlineKeyboardMarkup:
    buttons = []
    buttons.append([InlineKeyboardButton(text=f"🌐 Все собеседники ({total_count})", callback_data="fltr:all:0")])

    for chat_id, title, cnt in chats:
        display_title = title if len(title) <= 25 else title[:22] + "..."
        btn_text = f"👤 {display_title} ({cnt})"
        buttons.append([InlineKeyboardButton(text=btn_text, callback_data=f"fltr:{chat_id}:0")])

    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_pagination_keyboard(target_chat: str, current_idx: int, total_count: int) -> InlineKeyboardMarkup:
    buttons = []
    nav_row = []
    if current_idx > 0:
        nav_row.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"fltr:{target_chat}:{current_idx - 1}"))
    nav_row.append(InlineKeyboardButton(text=f"{current_idx + 1} / {total_count}", callback_data="noop"))
    if current_idx < total_count - 1:
        nav_row.append(InlineKeyboardButton(text="Вперёд ➡️", callback_data=f"fltr:{target_chat}:{current_idx + 1}"))
    buttons.append(nav_row)

    buttons.append([InlineKeyboardButton(text="👥 Выбрать другого собеседника", callback_data="del_menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def is_user_allowed(user_id: int) -> bool:
    if not ALLOWED_USERS:
        return True
    return user_id in ALLOWED_USERS

# ================= КОМАНДЫ =================

@router.message(Command("start"))
async def cmd_start(message: Message):
    if not is_user_allowed(message.from_user.id):
        await message.answer("⛔ У вас нет доступа к этому боту.")
        return

    await message.answer(
        "👋 **Бот архивации и контроля сообщений готов к работе**\n\n"
        "• При удалении вы **сразу получаете карточку** (фото, голос, текст).\n"
        "• В кнопке **«🗑 Удалённые сообщения»** доступен выбор собеседника для удобной фильтрации.\n"
        "• Все чаты полностью изолированы.",
        reply_markup=get_main_keyboard()
    )

@router.message(F.text == "ℹ️ Помощь")
async def cmd_help(message: Message):
    await message.answer(
        "📌 **Справка:**\n\n"
        "1. Подключите бота в **Настройки → Telegram для бизнеса → Чат-боты**.\n"
        "2. Бот сохраняет входящие и исходящие сообщения.\n"
        "3. **При удалении:** бот мгновенно присылает карточку с удалённым сообщением.\n"
        "4. **Архив:** нажмите «🗑 Удалённые сообщения», выберите нужного собеседника и листайте сообщения.",
        reply_markup=get_main_keyboard()
    )

@router.message(F.text == "📊 Статистика архива")
async def cmd_stats(message: Message):
    uid = message.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM messages WHERE owner_user_id = ? OR owner_user_id = 0", (uid,)) as cur:
            total_msgs = (await cur.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM messages WHERE (owner_user_id = ? OR owner_user_id = 0) AND is_deleted = 1", (uid,)) as cur:
            del_msgs = (await cur.fetchone())[0]
        async with db.execute("""
            SELECT COUNT(DISTINCT m.id)
            FROM messages m
            JOIN message_versions v ON m.id = v.message_ref_id
            WHERE owner_user_id = ? OR owner_user_id = 0
            GROUP BY m.id HAVING COUNT(v.id) > 1
        """, (uid,)) as cur:
            edited_msgs = len(await cur.fetchall())

    await message.answer(
        f"📊 **Ваша личная статистика архива:**\n\n"
        f"💬 Сообщений сохранено: **{total_msgs}**\n"
        f"🗑 Удалённых сообщений: **{del_msgs}**\n"
        f"✏️ Отредактированных: **{edited_msgs}**",
        reply_markup=get_main_keyboard()
    )

# ================= МЕНЮ ВЫБОРА СОБЕСЕДНИКА =================

async def show_chats_filter_menu(chat_id: int, bot: Bot, uid: int, edit_message_id: Optional[int] = None):
    chats = await get_deleted_chats_filter(uid)
    if not chats:
        text = "📭 В вашем архиве пока нет удалённых сообщений."
        if edit_message_id:
            try:
                await bot.edit_message_text(chat_id=chat_id, message_id=edit_message_id, text=text)
                return
            except Exception:
                pass
        await bot.send_message(chat_id=chat_id, text=text, reply_markup=get_main_keyboard())
        return

    total_deleted = sum(cnt for _, _, cnt in chats)
    text = (
        "🔍 **Выберите собеседника для просмотра удалённых сообщений:**\n\n"
        f"Всего собеседников с удалёнными сообщениями: **{len(chats)}**\n"
        f"Всего удалено сообщений: **{total_deleted}**"
    )
    kb = build_chats_filter_keyboard(chats, total_deleted)

    if edit_message_id:
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=edit_message_id, text=text, reply_markup=kb)
            return
        except Exception:
            pass

    await bot.send_message(chat_id=chat_id, text=text, reply_markup=kb)

@router.message(F.text == "🗑 Удалённые сообщения")
async def on_click_deleted_messages(message: Message):
    uid = message.from_user.id
    await show_chats_filter_menu(message.chat.id, message.bot, uid)

@router.callback_query(F.data == "del_menu")
async def on_back_to_del_menu(query: CallbackQuery):
    uid = query.from_user.id
    try:
        await query.message.delete()
    except Exception:
        pass
    await show_chats_filter_menu(query.message.chat.id, query.bot, uid)
    await query.answer()

# ================= ЛИСТАНИЕ УДАЛЁННЫХ СООБЩЕНИЙ ПО ФИЛЬТРУ =================

async def show_deleted_card_by_filter(chat_id: int, bot: Bot, uid: int, target_chat: str, idx: int):
    items = await get_deleted_messages_by_chat(uid, target_chat)
    if not items or idx >= len(items):
        await bot.send_message(chat_id=chat_id, text="📭 Сообщения не найдены.")
        return

    item = items[idx]
    filter_label = "Все чаты" if target_chat == "all" else item['chat_title']
    text_content = (
        f"🗑 **Удалённое сообщение [{idx + 1} из {len(items)}]**\n"
        f"📁 Фильтр: **{filter_label}**\n\n"
        f"💬 **Чат:** {item['chat_title']}\n"
        f"👤 **Автор:** {item['sender_name']}\n"
        f"🕒 **Отправлено:** {item['created_at']}\n"
        f"❌ **Удалено:** {item['deleted_at']}\n\n"
        f"📝 **Текст / Содержимое:**\n{item['text']}"
    )
    keyboard = get_pagination_keyboard(target_chat, idx, len(items))

    if item["media_type"] == "photo" and item["media_path"] and os.path.exists(item["media_path"]):
        await bot.send_photo(chat_id=chat_id, photo=FSInputFile(item["media_path"]), caption=text_content, reply_markup=keyboard)
    elif item["media_type"] == "voice" and item["media_path"] and os.path.exists(item["media_path"]):
        await bot.send_voice(chat_id=chat_id, voice=FSInputFile(item["media_path"]), caption=text_content, reply_markup=keyboard)
    elif item["media_type"] == "video_note" and item["media_path"] and os.path.exists(item["media_path"]):
        await bot.send_video_note(chat_id=chat_id, video_note=FSInputFile(item["media_path"]))
        await bot.send_message(chat_id=chat_id, text=text_content, reply_markup=keyboard)
    else:
        await bot.send_message(chat_id=chat_id, text=text_content, reply_markup=keyboard)

@router.callback_query(F.data.startswith("fltr:"))
async def on_filter_pagination(query: CallbackQuery):
    parts = query.data.split(":")
    target_chat = parts[1]
    idx = int(parts[2])
    uid = query.from_user.id

    try:
        await query.message.delete()
    except Exception:
        pass

    await show_deleted_card_by_filter(query.message.chat.id, query.bot, uid, target_chat, idx)
    await query.answer()

# ================= ПРОСМОТР ИЗМЕНЁННЫХ СООБЩЕНИЙ =================

@router.message(F.text == "✏️ Изменённые сообщения")
async def view_edited_list(message: Message):
    uid = message.from_user.id
    items = await get_edited_messages_list(uid)
    if not items:
        await message.answer("📭 В вашем архиве пока нет отредактированных сообщений.", reply_markup=get_main_keyboard())
        return
    await show_edited_card(message.chat.id, message.bot, uid, 0, items)

def get_edit_pagination_keyboard(current_idx: int, total_count: int) -> InlineKeyboardMarkup:
    buttons = []
    nav_row = []
    if current_idx > 0:
        nav_row.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"edit:{current_idx - 1}"))
    nav_row.append(InlineKeyboardButton(text=f"{current_idx + 1} / {total_count}", callback_data="noop"))
    if current_idx < total_count - 1:
        nav_row.append(InlineKeyboardButton(text="Вперёд ➡️", callback_data=f"edit:{current_idx + 1}"))
    buttons.append(nav_row)
    return InlineKeyboardMarkup(inline_keyboard=buttons)

async def show_edited_card(chat_id: int, bot: Bot, uid: int, idx: int, items: List[dict], edit_message_id: Optional[int] = None):
    item = items[idx]
    lines = [
        f"✏️ **Изменённое сообщение [{idx + 1} из {len(items)}]**\n",
        f"💬 **Чат:** {item['chat_title']}",
        f"👤 **Автор:** {item['sender_name']}",
        f"🕒 **Создано:** {item['created_at']}\n",
        "**Все версии текста:**"
    ]
    for v in item["versions"]:
        title = "Исходный текст" if v["version"] == 1 else f"Версия #{v['version']}"
        lines.append(f"• **{title}** ({v['saved_at']}):\n{v['text']}\n")

    text_content = "\n".join(lines)
    keyboard = get_edit_pagination_keyboard(idx, len(items))

    if edit_message_id:
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=edit_message_id, text=text_content, reply_markup=keyboard)
            return
        except Exception:
            pass

    await bot.send_message(chat_id=chat_id, text=text_content, reply_markup=keyboard)

@router.callback_query(F.data.startswith("edit:"))
async def on_edit_pagination(query: CallbackQuery):
    idx = int(query.data.split(":")[1])
    uid = query.from_user.id
    items = await get_edited_messages_list(uid)
    if not items or idx >= len(items):
        await query.answer("Не найдено.")
        return
    await show_edited_card(query.message.chat.id, query.bot, uid, idx, items, edit_message_id=query.message.message_id)
    await query.answer()

@router.callback_query(F.data == "noop")
async def on_noop(query: CallbackQuery):
    await query.answer()

# ================= РЕГИСТРАЦИЯ БИЗНЕС-ПОДКЛЮЧЕНИЙ =================

if hasattr(router, "business_connection"):
    @router.business_connection()
    async def on_business_conn(conn: BusinessConnection):
        uname = conn.user.full_name or "Пользователь"
        if conn.user.username:
            uname += f" (@{conn.user.username})"
        await save_business_connection(conn.id, conn.user.id, uname)
else:
    @router.update(F.business_connection)
    async def on_business_conn_update(update: Update):
        conn = update.business_connection
        uname = conn.user.full_name or "Пользователь"
        if conn.user.username:
            uname += f" (@{conn.user.username})"
        await save_business_connection(conn.id, conn.user.id, uname)

# ================= ОБРАБОТКА СООБЩЕНИЙ =================

async def process_msg(message: Message, bot: Bot):
    text, m_type, m_path = await extract_and_download_media(message, bot)
    if not text and not m_type:
        return

    chat_id, message_id, sender_id, sender_name, chat_title = extract_sender_info(message)
    b_conn_id = getattr(message, "business_connection_id", None)
    owner_id = await get_connection_owner(b_conn_id, bot)

    if not owner_id:
        if len(ALLOWED_USERS) == 1:
            owner_id = ALLOWED_USERS[0]
        elif message.from_user and message.from_user.id in ALLOWED_USERS:
            owner_id = message.from_user.id
        elif ALLOWED_USERS:
            owner_id = ALLOWED_USERS[0]
        else:
            owner_id = chat_id

    await save_new_message(
        owner_user_id=owner_id,
        chat_id=chat_id,
        message_id=message_id,
        sender_id=sender_id,
        sender_name=sender_name,
        chat_title=chat_title,
        text=text,
        media_type=m_type,
        media_path=m_path,
        business_connection_id=b_conn_id
    )

@router.message()
async def on_new_message(message: Message, bot: Bot):
    await process_msg(message, bot)

@router.edited_message()
async def on_edited_message(message: Message):
    text = message.text or message.caption or "[Медиа]"
    await save_edited_message(message.chat.id, message.message_id, text)

@router.business_message()
async def on_business_message(message: Message, bot: Bot):
    await process_msg(message, bot)

@router.edited_business_message()
async def on_edited_business_message(message: Message):
    text = message.text or message.caption or "[Медиа]"
    await save_edited_message(message.chat.id, message.message_id, text)

# ================= МГНОВЕННЫЕ УВЕДОМЛЕНИЯ ПРИ УДАЛЕНИИ =================

async def send_deleted_notification(bot: Bot, item: dict, target_user_id: int):
    caption_text = (
        "🗑 **Удалено сообщение**\n\n"
        f"👤 **Кто написал:** {item['sender_name']}\n"
        f"💬 **В каком чате:** {item['chat_title']}\n"
        f"🕒 **Отправлено:** {item['created_at']}\n"
        f"❌ **Удалено:** {item['deleted_at']}\n\n"
        f"📝 **Текст / Содержимое:**\n{item['latest_text']}"
    )

    m_type = item.get("media_type")
    m_path = item.get("media_path")

    # Фото
    if m_type == "photo" and m_path and os.path.exists(m_path):
        try:
            await bot.send_photo(chat_id=target_user_id, photo=FSInputFile(m_path), caption=caption_text)
            return
        except Exception:
            pass

    # Голос
    if m_type == "voice" and m_path and os.path.exists(m_path):
        try:
            await bot.send_voice(chat_id=target_user_id, voice=FSInputFile(m_path), caption=caption_text)
            return
        except Exception:
            pass

    # Кружочек
    if m_type == "video_note" and m_path and os.path.exists(m_path):
        try:
            await bot.send_video_note(chat_id=target_user_id, video_note=FSInputFile(m_path))
            await bot.send_message(chat_id=target_user_id, text=caption_text)
            return
        except Exception:
            pass

    # File ID
    if m_type == "photo" and m_path:
        try:
            await bot.send_photo(chat_id=target_user_id, photo=m_path, caption=caption_text)
            return
        except Exception:
            pass

    if m_type == "video" and m_path:
        try:
            await bot.send_video(chat_id=target_user_id, video=m_path, caption=caption_text)
            return
        except Exception:
            pass

    try:
        await bot.send_message(chat_id=target_user_id, text=caption_text)
    except Exception as e:
        logger.error(f"Не удалось отправить уведомление об удалении {target_user_id}: {e}")

async def handle_deleted(chat_id: int, message_ids: list, b_conn_id: Optional[str], bot: Bot):
    owner_id = await get_connection_owner(b_conn_id, bot)
    if not owner_id:
        if len(ALLOWED_USERS) == 1:
            owner_id = ALLOWED_USERS[0]
        elif ALLOWED_USERS:
            owner_id = ALLOWED_USERS[0]
        else:
            owner_id = chat_id

    records = await mark_and_get_deleted_messages(chat_id, message_ids, owner_id)
    for rec in records:
        if rec["found"]:
            await send_deleted_notification(bot, rec, rec["owner_user_id"])
        else:
            notif = (
                "🗑 **Удалено сообщение**\n\n"
                f"💬 **Чат:** {chat_id}\n"
                f"🆔 **ID сообщения:** #{rec['message_id']}\n"
                "ℹ️ Текст не найден в архиве (отправлено до запуска бота)."
            )
            try:
                await bot.send_message(chat_id=owner_id, text=notif)
            except Exception as e:
                logger.error(f"Ошибка отправки: {e}")

if hasattr(router, "deleted_business_messages"):
    @router.deleted_business_messages()
    async def on_del(event: BusinessMessagesDeleted, bot: Bot):
        await handle_deleted(event.chat.id, event.message_ids, event.business_connection_id, bot)
else:
    @router.update(F.deleted_business_messages)
    async def on_del_upd(update: Update, bot: Bot):
        ev = update.deleted_business_messages
        await handle_deleted(ev.chat.id, ev.message_ids, ev.business_connection_id, bot)

# ================= ЗАПУСК =================

async def main():
    validate_storage()
    if MAINTENANCE_MODE:
        logger.info("Режим переноса: Telegram polling выключен; Volume готов. Для запуска задайте MAINTENANCE_MODE=0.")
        await asyncio.Event().wait()
        return
    await init_db()
    logger.info("Хранилище готово. SQLite: %s; медиа: %s", DB_PATH, MEDIA_DIR)
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN))
    dp = Dispatcher()
    dp.include_router(router)

    allowed_updates = [
        "message",
        "edited_message",
        "business_connection",
        "business_message",
        "edited_business_message",
        "deleted_business_messages",
        "callback_query",
    ]

    logger.info("Запуск Telegram polling. Фильтр по собеседникам включен...")
    await dp.start_polling(bot, allowed_updates=allowed_updates)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот остановлен.")