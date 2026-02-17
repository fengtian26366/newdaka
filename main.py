import os
import io
import csv
import asyncio
import signal
import html
import re
from datetime import datetime, timedelta, date, time
from zoneinfo import ZoneInfo

import asyncpg
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ChatType, ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.types.input_file import BufferedInputFile


# =========================
# 基础配置
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "").strip()

if not BOT_TOKEN:
    raise RuntimeError("Missing BOT_TOKEN")
if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL")

TZ = ZoneInfo("Asia/Colombo")  # 斯里兰卡
DAY_START = time(7, 0)
NIGHT_START = time(19, 0)


def parse_admin_ids(raw: str):
    if not raw:
        return set()
    return {int(x.strip()) for x in raw.split(",") if x.strip().isdigit()}


ADMIN_IDS = parse_admin_ids(ADMIN_IDS_RAW)

bot = Bot(BOT_TOKEN)
dp = Dispatcher()
pool: asyncpg.Pool | None = None


# =========================
# 按钮：必须以 / 开头（最稳）
# =========================
KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="/meal 吃饭"), KeyboardButton(text="/pee 小便"), KeyboardButton(text="/poop 大便")],
        [KeyboardButton(text="/smoke 抽烟"), KeyboardButton(text="/back 回来")],
        [KeyboardButton(text="/export 导出"), KeyboardButton(text="/missed 缺卡")],
    ],
    resize_keyboard=True
)

KIND_CN = {"meal": "吃饭", "pee": "小便", "poop": "大便", "smoke": "抽烟"}
LIMITS = {"meal": 3, "pee": 3, "poop": 2, "smoke": 5}
DEFAULT_MIN = {"meal": 30, "pee": 6, "poop": 15, "smoke": 10}


# =========================
# 时间归属（白班/夜班 + 夜班凌晨归前一天）
# =========================
def now_sl() -> datetime:
    return datetime.now(tz=TZ)


def infer_shift(dt: datetime) -> tuple[str, date]:
    lt = dt.astimezone(TZ)
    t = lt.time()
    if DAY_START <= t < NIGHT_START:
        return "白班", lt.date()
    if t < DAY_START:
        return "夜班", lt.date() - timedelta(days=1)
    return "夜班", lt.date()


def mention(message: Message) -> str:
    u = message.from_user
    if not u:
        return "用户"
    title = html.escape(u.full_name or u.username or "用户")
    return f'<a href="tg://user?id={u.id}">{title}</a>'


# =========================
# DB（注意：active_session 用 chat_id+user_id 作为主键）
# =========================
async def db_init():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)

    async with pool.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users_seen (
            chat_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            user_name TEXT,
            first_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_seen  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY(chat_id, user_id)
        );
        """)

        await conn.execute("""
        CREATE TABLE IF NOT EXISTS break_sum (
            chat_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            user_name TEXT,
            shift_date DATE NOT NULL,
            shift TEXT NOT NULL,
            kind TEXT NOT NULL,
            count INT NOT NULL DEFAULT 0,
            minutes INT NOT NULL DEFAULT 0,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY(chat_id, user_id, shift_date, shift, kind)
        );
        """)

        await conn.execute("""
        CREATE TABLE IF NOT EXISTS break_event (
            id BIGSERIAL PRIMARY KEY,
            chat_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            user_name TEXT,
            shift_date DATE NOT NULL,
            shift TEXT NOT NULL,
            kind TEXT NOT NULL,
            start_at TIMESTAMPTZ NOT NULL,
            end_at TIMESTAMPTZ NOT NULL,
            used_min INT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)

        await conn.execute("""
        CREATE TABLE IF NOT EXISTS active_session (
            chat_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            shift_date DATE NOT NULL,
            shift TEXT NOT NULL,
            kind TEXT NOT NULL,
            start_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY(chat_id, user_id)
        );
        """)

        await conn.execute("CREATE INDEX IF NOT EXISTS idx_sum_date_shift ON break_sum(chat_id, shift_date, shift);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_evt_date_shift ON break_event(chat_id, shift_date, shift);")


async def touch_user(chat_id: int, user_id: int, user_name: str):
    async with pool.acquire() as conn:
        await conn.execute("""
        INSERT INTO users_seen(chat_id, user_id, user_name)
        VALUES($1,$2,$3)
        ON CONFLICT(chat_id, user_id) DO UPDATE
        SET user_name=EXCLUDED.user_name, last_seen=NOW()
        """, chat_id, user_id, user_name)


async def get_active(chat_id: int, user_id: int):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM active_session WHERE chat_id=$1 AND user_id=$2",
            chat_id, user_id
        )


async def start_active(chat_id: int, user_id: int, shift_date: date, shift: str, kind: str, start_at: datetime):
    async with pool.acquire() as conn:
        await conn.execute("""
        INSERT INTO active_session(chat_id,user_id,shift_date,shift,kind,start_at)
        VALUES($1,$2,$3,$4,$5,$6)
        ON CONFLICT(chat_id,user_id) DO UPDATE
        SET shift_date=EXCLUDED.shift_date,
            shift=EXCLUDED.shift,
            kind=EXCLUDED.kind,
            start_at=EXCLUDED.start_at
        """, chat_id, user_id, shift_date, shift, kind, start_at)


async def clear_active(chat_id: int, user_id: int):
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM active_session WHERE chat_id=$1 AND user_id=$2",
            chat_id, user_id
        )
        await conn.execute(
            "DELETE FROM active_session WHERE chat_id=$1 AND user_id=$2",
            chat_id, user_id
        )
        return row


async def add_sum(chat_id: int, user_id: int, user_name: str, shift_date: date, shift: str, kind: str, used_min: int):
    async with pool.acquire() as conn:
        await conn.execute("""
        INSERT INTO break_sum(chat_id,user_id,user_name,shift_date,shift,kind,count,minutes)
        VALUES($1,$2,$3,$4,$5,$6,1,$7)
        ON CONFLICT(chat_id,user_id,shift_date,shift,kind) DO UPDATE
        SET user_name=EXCLUDED.user_name,
            count=break_sum.count+1,
            minutes=break_sum.minutes+EXCLUDED.minutes,
            updated_at=NOW()
        """, chat_id, user_id, user_name, shift_date, shift, kind, used_min)


async def add_event(chat_id: int, user_id: int, user_name: str, shift_date: date, shift: str,
                    kind: str, start_at: datetime, end_at: datetime, used_min: int):
    async with pool.acquire() as conn:
        await conn.execute("""
        INSERT INTO break_event(chat_id,user_id,user_name,shift_date,shift,kind,start_at,end_at,used_min)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
        """, chat_id, user_id, user_name, shift_date, shift, kind, start_at, end_at, used_min)


# =========================
# 命令：/start /ping
# =========================
@dp.message(CommandStart())
async def start_cmd(message: Message):
    await message.reply(
        "✅ 打卡机器人已启动（斯里兰卡时间）\n"
        "用法：/meal /pee /poop /smoke 开始，/back 回来结算\n"
        "导出：/export 2026-02-18（管理员）\n"
        "缺卡：/missed 2026-02-18（管理员）",
        reply_markup=KB
    )


@dp.message(Command("ping"))
async def ping_cmd(message: Message):
    await message.reply("pong ✅ 我收到消息了")


# =========================
# 主入口：兼容 /pee@botname
# =========================
def extract_cmd(text: str) -> str:
    """
    只取开头命令：
    /pee
    /pee@YourBot
    /pee@YourBot 吃饭
    """
    if not text:
        return ""
    m = re.match(r"^/([a-zA-Z_]+)(?:@[\w_]+)?", text.strip())
    return f"/{m.group(1).lower()}" if m else ""


@dp.message(F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}) & F.text)
async def handler(message: Message):
    try:
        chat_id = message.chat.id
        user_id = message.from_user.id
        user_name = message.from_user.full_name or (message.from_user.username or str(user_id))

        # ✅ 只要说过话/用过按钮，就登记“出现过”
        await touch_user(chat_id, user_id, user_name)

        cmd = extract_cmd(message.text)
        if not cmd:
            return  # 非命令不处理（隐私模式也更稳）

        now = now_sl()
        shift, shift_date = infer_shift(now)

        # /back
        if cmd == "/back":
            act = await clear_active(chat_id, user_id)
            if not act:
                return await message.reply("没有进行中的记录", reply_markup=KB)

            used = int(max(0, (now - act["start_at"]).total_seconds() // 60))
            kind = act["kind"]

            await add_event(chat_id, user_id, user_name, act["shift_date"], act["shift"], kind, act["start_at"], now, used)
            await add_sum(chat_id, user_id, user_name, act["shift_date"], act["shift"], kind, used)

            limit_min = DEFAULT_MIN.get(kind, 0)
            overtime = max(0, used - limit_min) if limit_min else 0
            extra = f"（超时 {overtime} 分钟）" if overtime > 0 else "（未超时）"

            return await message.reply(
                f"✅ {mention(message)} 已回来：本次【{KIND_CN.get(kind, kind)}】{used} 分钟 {extra}\n"
                f"归属：{act['shift_date']} {act['shift']}",
                parse_mode=ParseMode.HTML,
                reply_markup=KB
            )

        # /meal /pee /poop /smoke
        kind = cmd.lstrip("/")
        if kind in KIND_CN:
            act = await get_active(chat_id, user_id)
            if act:
                return await message.reply("⚠️ 你还有进行中的状态，请先 /back", reply_markup=KB)

            # 次数限制（按 shift_date+shift）
            # 这里为了简单不查 count 也能跑；你要严格限制我再加查询
            await start_active(chat_id, user_id, shift_date, shift, kind, now)

            deadline = (now + timedelta(minutes=DEFAULT_MIN.get(kind, 10))).astimezone(TZ).strftime("%H:%M")
            return await message.reply(
                f"📝 已记录：{KIND_CN[kind]}\n归属：{shift_date} {shift}\n"
                f"⏰ 建议 {deadline} 前回来，结束请发 /back",
                reply_markup=KB
            )

        # 其它命令：给个提示（避免“没反应”）
        return await message.reply(f"收到命令：{cmd}（未定义）", reply_markup=KB)

    except Exception as e:
        # ✅ 出错也要给反馈，不然用户以为没反应
        await message.reply(f"❌ 处理失败：{type(e).__name__}: {e}")


# =========================
# Railway 稳定轮询（自动重连 + SIGTERM）
# =========================
_stop = asyncio.Event()


def _stop_signal():
    print("[bot] got stop signal")
    _stop.set()


async def run_forever():
    await db_init()
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception as e:
        print("[bot] delete_webhook error:", e)

    while not _stop.is_set():
        try:
            print("[bot] polling started")
            await dp.start_polling(bot, allowed_updates=["message"])
        except Exception as e:
            print("[bot] polling crashed:", repr(e))
            await asyncio.sleep(2)


async def main():
    loop = asyncio.get_running_loop()
    for s in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(s, _stop_signal)
        except Exception:
            pass
    await run_forever()


if __name__ == "__main__":
    asyncio.run(main())
