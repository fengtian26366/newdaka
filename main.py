import os
import io
import csv
import asyncio
import signal
import html
from datetime import datetime, timedelta, date, time
from zoneinfo import ZoneInfo

import asyncpg
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ChatType, ParseMode
from aiogram.filters import Command, CommandStart
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

TZ = ZoneInfo("Asia/Colombo")

DAY_START = time(7, 0)
NIGHT_START = time(19, 0)


def parse_admin_ids(raw: str):
    if not raw:
        return set()
    return {int(x.strip()) for x in raw.split(",") if x.strip().isdigit()}


ADMIN_IDS = parse_admin_ids(ADMIN_IDS_RAW)

bot = Bot(BOT_TOKEN)
dp = Dispatcher()
pool = None


# =========================
# 按钮（必须以 / 开头）
# =========================
KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="/meal 吃饭"), KeyboardButton(text="/pee 小便"), KeyboardButton(text="/poop 大便")],
        [KeyboardButton(text="/smoke 抽烟"), KeyboardButton(text="/back 回来")],
        [KeyboardButton(text="/export 导出"), KeyboardButton(text="/missed 缺卡")],
    ],
    resize_keyboard=True
)


KIND_CN = {
    "meal": "吃饭",
    "pee": "小便",
    "poop": "大便",
    "smoke": "抽烟"
}

LIMITS = {
    "meal": 3,
    "pee": 3,
    "poop": 2,
    "smoke": 5
}

DEFAULT_MIN = {
    "meal": 30,
    "pee": 6,
    "poop": 15,
    "smoke": 10
}


# =========================
# 时间归属逻辑
# =========================
def now_sl():
    return datetime.now(tz=TZ)


def infer_shift(dt):
    t = dt.time()
    if DAY_START <= t < NIGHT_START:
        return "白班", dt.date()
    if t < DAY_START:
        return "夜班", dt.date() - timedelta(days=1)
    return "夜班", dt.date()


# =========================
# DB 初始化
# =========================
async def db_init():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)

    async with pool.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS shift_sum (
            chat_id BIGINT,
            user_id BIGINT,
            user_name TEXT,
            shift_date DATE,
            shift TEXT,
            kind TEXT,
            count INT DEFAULT 0,
            minutes INT DEFAULT 0,
            PRIMARY KEY(chat_id, user_id, shift_date, shift, kind)
        );
        """)

        await conn.execute("""
        CREATE TABLE IF NOT EXISTS active_session (
            chat_id BIGINT PRIMARY KEY,
            user_id BIGINT,
            shift_date DATE,
            shift TEXT,
            kind TEXT,
            start_at TIMESTAMPTZ
        );
        """)


# =========================
# 工具函数
# =========================
def mention(message: Message):
    name = html.escape(message.from_user.full_name)
    return f'<a href="tg://user?id={message.from_user.id}">{name}</a>'


async def get_active(chat_id):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM active_session WHERE chat_id=$1",
            chat_id
        )


async def clear_active(chat_id):
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM active_session WHERE chat_id=$1",
            chat_id
        )
        await conn.execute(
            "DELETE FROM active_session WHERE chat_id=$1",
            chat_id
        )
        return row


async def start_active(chat_id, user_id, shift_date, shift, kind, start_at):
    async with pool.acquire() as conn:
        await conn.execute("""
        INSERT INTO active_session(chat_id,user_id,shift_date,shift,kind,start_at)
        VALUES($1,$2,$3,$4,$5,$6)
        ON CONFLICT(chat_id)
        DO UPDATE SET
        user_id=EXCLUDED.user_id,
        shift_date=EXCLUDED.shift_date,
        shift=EXCLUDED.shift,
        kind=EXCLUDED.kind,
        start_at=EXCLUDED.start_at
        """, chat_id, user_id, shift_date, shift, kind, start_at)


async def add_record(chat_id, user_id, user_name, shift_date, shift, kind, minutes):
    async with pool.acquire() as conn:
        await conn.execute("""
        INSERT INTO shift_sum(chat_id,user_id,user_name,shift_date,shift,kind,count,minutes)
        VALUES($1,$2,$3,$4,$5,$6,1,$7)
        ON CONFLICT(chat_id,user_id,shift_date,shift,kind)
        DO UPDATE SET
        count = shift_sum.count + 1,
        minutes = shift_sum.minutes + EXCLUDED.minutes
        """, chat_id, user_id, user_name, shift_date, shift, kind, minutes)


# =========================
# 命令
# =========================
@dp.message(CommandStart())
async def start_cmd(message: Message):
    await message.reply("✅ 打卡机器人已启动（斯里兰卡时间）", reply_markup=KB)


@dp.message(Command("ping"))
async def ping_cmd(message: Message):
    await message.reply("pong ✅ 机器人在线")


# =========================
# 主逻辑
# =========================
@dp.message(F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
async def handler(message: Message):
    if not message.text:
        return

    text = message.text.strip().lower()

    if not text.startswith("/"):
        return

    cmd = text.split()[0]

    chat_id = message.chat.id
    user_id = message.from_user.id
    user_name = message.from_user.full_name
    now = now_sl()
    shift, shift_date = infer_shift(now)

    # 结束
    if cmd == "/back":
        act = await clear_active(chat_id)
        if not act:
            return await message.reply("没有进行中的记录", reply_markup=KB)

        used = int((now - act["start_at"]).total_seconds() // 60)
        await add_record(chat_id, user_id, user_name, act["shift_date"], act["shift"], act["kind"], used)

        return await message.reply(
            f"✅ {mention(message)} 本次 {KIND_CN[act['kind']]} 用时 {used} 分钟\n归属 {act['shift_date']} {act['shift']}",
            parse_mode=ParseMode.HTML,
            reply_markup=KB
        )

    # 休息类
    if cmd.replace("/", "") in KIND_CN:
        kind = cmd.replace("/", "")

        act = await get_active(chat_id)
        if act:
            return await message.reply("⚠️ 还有进行中的状态，请先 /back", reply_markup=KB)

        await start_active(chat_id, user_id, shift_date, shift, kind, now)

        return await message.reply(
            f"📝 已记录 {KIND_CN[kind]}（归属 {shift_date} {shift}）",
            reply_markup=KB
        )


# =========================
# Railway 稳定轮询
# =========================
_stop = asyncio.Event()


def _stop_signal():
    _stop.set()


async def run():
    await db_init()
    await bot.delete_webhook(drop_pending_updates=True)

    while not _stop.is_set():
        try:
            print("polling started")
            await dp.start_polling(bot, allowed_updates=["message"])
        except Exception as e:
            print("polling crash:", e)
            await asyncio.sleep(2)


async def main():
    loop = asyncio.get_running_loop()
    for s in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(s, _stop_signal)
        except:
            pass
    await run()


if __name__ == "__main__":
    asyncio.run(main())
