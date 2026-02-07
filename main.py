import os
import io
import re
import csv
import asyncio
import html
from datetime import datetime, timedelta, date, time
from zoneinfo import ZoneInfo
from typing import Optional

import asyncpg
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.enums import ChatType, ParseMode
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

# ✅ 越南时间（UTC+7）
TZ = ZoneInfo("Asia/Ho_Chi_Minh")


def parse_admin_ids(raw: str) -> set[int]:
    if not raw:
        return set()
    out = set()
    for x in raw.split(","):
        x = x.strip()
        if x.isdigit():
            out.add(int(x))
    return out


ADMIN_IDS = parse_admin_ids(ADMIN_IDS_RAW)

bot = Bot(BOT_TOKEN)
dp = Dispatcher()
pool: asyncpg.Pool | None = None


# =========================
# 文本识别 + 键盘
# =========================
TEXT_ALIASES = {
    "上班": "start",
    "开工": "start",
    "in": "start",

    "下班": "end",
    "收工": "end",
    "out": "end",

    "导出": "export",
}

KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="上班"), KeyboardButton(text="下班")],
        [KeyboardButton(text="导出")],
    ],
    resize_keyboard=True
)


def now_vn() -> datetime:
    return datetime.now(tz=TZ)


def normalize_text(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", "", s)
    return s


def get_tg_name(message: Message) -> str:
    u = message.from_user
    if not u:
        return ""
    name = (u.full_name or "").strip()
    if name:
        return name
    if u.username:
        return f"@{u.username}"
    return str(u.id)


def mention_html(message: Message) -> str:
    u = message.from_user
    if not u:
        return "用户"
    title = html.escape((u.full_name or u.username or "用户"))
    return f'<a href="tg://user?id={u.id}">{title}</a>'


# =========================
# DB 初始化：上下班记录表
# =========================
async def db_init():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)

    async with pool.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS work_log (
            id BIGSERIAL PRIMARY KEY,
            chat_id BIGINT NOT NULL,
            tg_user_id BIGINT NOT NULL,
            tg_name TEXT,
            start_at TIMESTAMPTZ NOT NULL,
            end_at   TIMESTAMPTZ,
            minutes  INT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)
        await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_work_log_chat_start
        ON work_log(chat_id, start_at);
        """)
        await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_work_log_open
        ON work_log(chat_id, tg_user_id)
        WHERE end_at IS NULL;
        """)


# =========================
# DB 工具
# =========================
async def get_open_shift(chat_id: int, tg_user_id: int):
    async with pool.acquire() as conn:
        return await conn.fetchrow("""
            SELECT * FROM work_log
            WHERE chat_id=$1 AND tg_user_id=$2 AND end_at IS NULL
            ORDER BY start_at DESC
            LIMIT 1
        """, chat_id, tg_user_id)


async def start_shift(chat_id: int, tg_user_id: int, tg_name: str, start_at: datetime):
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO work_log(chat_id, tg_user_id, tg_name, start_at)
            VALUES($1,$2,$3,$4)
        """, chat_id, tg_user_id, tg_name, start_at)


async def end_shift(log_id: int, end_at: datetime, minutes: int, tg_name: str):
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE work_log
            SET end_at=$1, minutes=$2, tg_name=$3
            WHERE id=$4
        """, end_at, minutes, tg_name, log_id)


async def fetch_export_day(chat_id: int, day: date):
    """
    ✅ 导出“当天记录”（越南自然日 00:00~23:59）
    口径：只要这条上下班记录在当天有发生（start_at 落在当天）
    更直观，适合“当天打卡记录”。
    """
    start_dt = datetime.combine(day, time(0, 0), TZ)
    end_dt = start_dt + timedelta(days=1)

    async with pool.acquire() as conn:
        return await conn.fetch("""
            SELECT tg_user_id, tg_name, start_at, end_at, minutes
            FROM work_log
            WHERE chat_id=$1
              AND start_at >= $2 AND start_at < $3
            ORDER BY start_at ASC, tg_user_id ASC
        """, chat_id, start_dt, end_dt)


# =========================
# 指令：/start /export
# =========================
@dp.message(CommandStart())
async def start_cmd(message: Message):
    if message.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.reply(
            "✅ 上下班打卡机器人已启用（按越南时间 UTC+7）\n\n"
            "按钮：上班 / 下班 / 导出\n"
            "规则：不限制打卡时间；上班后才能下班；未下班前不能重复上班。\n\n"
            "导出当天：/export 2026-02-05\n"
            "不写日期：/export  （默认导出今天）",
            reply_markup=KB
        )
    else:
        await message.reply("请把机器人拉进群使用。", reply_markup=KB)


@dp.message(Command("export"))
async def export_cmd(message: Message):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return await message.reply("请在群里导出。")

    if ADMIN_IDS and (message.from_user.id not in ADMIN_IDS):
        return await message.reply("你没有导出权限。")

    parts = (message.text or "").split()

    def parse_d(s: str) -> Optional[date]:
        try:
            return datetime.strptime(s, "%Y-%m-%d").date()
        except Exception:
            return None

    if len(parts) >= 2:
        d = parse_d(parts[1])
        if not d:
            return await message.reply("格式：/export 2026-02-05")
        day = d
    else:
        day = now_vn().date()  # 默认今天（越南）

    wait = await message.reply("⏳ 正在导出请稍等…")
    try:
        rows = await fetch_export_day(message.chat.id, day)

        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow([
            "日期(越南)",
            "用户ID",
            "用户名",
            "上班时间(越南)",
            "下班时间(越南)",
            "工作时长(分钟)",
            "工作时长(小时)"
        ])

        for r in rows:
            uid_text = "\t" + str(int(r["tg_user_id"]))  # 防止Excel科学计数法
            name_text = (r["tg_name"] or "").strip()

            s_at = r["start_at"].astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S") if r["start_at"] else ""
            e_at = r["end_at"].astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S") if r["end_at"] else ""

            mins = r["minutes"]
            mins_text = "" if mins is None else str(int(mins))
            hours_text = "" if mins is None else f"{(mins/60):.2f}"

            w.writerow([str(day), uid_text, name_text, s_at, e_at, mins_text, hours_text])

        data = buf.getvalue().encode("utf-8-sig")
        filename = f"上下班打卡_{message.chat.id}_{day}.csv"
        doc = BufferedInputFile(data, filename=filename)
        await message.answer_document(doc)
        await wait.edit_text("✅ 导出完成")
    except Exception as e:
        await wait.edit_text(f"❌ 导出失败：{type(e).__name__}: {e}")


# =========================
# 群消息：上班/下班/导出（按钮或文字）
# =========================
@dp.message(F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}) & F.text)
async def on_group_text(message: Message):
    raw = (message.text or "").strip()
    key = normalize_text(raw)

    kind = TEXT_ALIASES.get(raw) or TEXT_ALIASES.get(key)
    if not kind:
        return

    chat_id = message.chat.id
    tg_user_id = message.from_user.id
    tg_name = get_tg_name(message)
    mention = mention_html(message)

    now = now_vn()

    # “导出”按钮：等同于导出今天
    if kind == "export":
        # 直接复用 /export 的逻辑：这里手动调用一个等价输出
        fake = Message.model_validate(message.model_dump())
        fake.text = "/export"
        return await export_cmd(fake)

    # 上班
    if kind == "start":
        open_row = await get_open_shift(chat_id, tg_user_id)
        if open_row:
            s = open_row["start_at"].astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S")
            return await message.reply(
                f"⛔️ {mention} 你已在上班中（上班时间：{s}），请先点【下班】。",
                reply_markup=KB,
                parse_mode=ParseMode.HTML
            )
        await start_shift(chat_id, tg_user_id, tg_name, now)
        return await message.answer(
            f"✅ {mention} 上班打卡成功（越南时间）：{now.astimezone(TZ).strftime('%Y-%m-%d %H:%M:%S')}",
            reply_markup=KB,
            parse_mode=ParseMode.HTML
        )

    # 下班
    if kind == "end":
        open_row = await get_open_shift(chat_id, tg_user_id)
        if not open_row:
            return await message.reply(
                f"⛔️ {mention} 你还没有上班记录，无法下班。请先点【上班】。",
                reply_markup=KB,
                parse_mode=ParseMode.HTML
            )

        start_at = open_row["start_at"]
        used_min = int(max(0, (now - start_at).total_seconds() // 60))
        await end_shift(int(open_row["id"]), now, used_min, tg_name)

        return await message.answer(
            f"✅ {mention} 下班打卡成功（越南时间）：{now.astimezone(TZ).strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"⏱ 本次工作时长：{used_min} 分钟（{used_min/60:.2f} 小时）",
            reply_markup=KB,
            parse_mode=ParseMode.HTML
        )


# =========================
# 启动
# =========================
async def main():
    await db_init()
    await bot.delete_webhook(drop_pending_updates=True)
    print("[bot] polling started")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
