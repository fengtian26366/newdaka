import os
import io
import re
import csv
import asyncio
import html
import random
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from typing import Optional, Tuple

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

# ✅ 斯里兰卡时间
TZ = ZoneInfo("Asia/Colombo")

def parse_admin_ids(raw: str) -> set[int]:
    out: set[int] = set()
    if not raw:
        return out
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
# 业务规则（每段刷新：07:00 & 19:00）
# =========================
DAILY_LIMITS = {
    "pee": 3,
    "poop": 2,
    "meal": 3,
    "smoke": 5,
}

DEFAULT_MINUTES = {
    "pee": 6,
    "poop": 15,
    "meal": 30,
    "smoke": 10,
}

KIND_CN = {
    "pee": "小便/厕所",
    "poop": "大便",
    "meal": "吃饭",
    "smoke": "抽烟",
}

SEG_CN = {
    "DAY": "白班段(07:00-19:00)",
    "NIGHT": "晚班段(19:00-次日07:00)",
}

QUOTES_START = [
    "📝 已记录，注意时间哦。",
    "✅ 好的，回来记得点 /back。",
    "⏳ 计时开始，别超时～",
    "📌 已开始计时。",
]
QUOTES_BACK = [
    "✅ 已回来，继续工作。",
    "👏 回来啦，辛苦。",
    "💪 继续保持节奏。",
    "✅ 记录完成。",
]

# ✅ 命令映射（隐私模式也能用）
CMD_ALIASES = {
    "/meal": "meal",
    "/pee": "pee",
    "/poop": "poop",
    "/smoke": "smoke",
    "/back": "back",
    "/export": "export",
}

TEXT_ALIASES = {
    "吃饭": "meal",
    "小便": "pee",
    "厕所": "pee",
    "大便": "poop",
    "抽烟": "smoke",
    "回来": "back",
    "回": "back",
    "back": "back",
    "导出": "export",
}

KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🍚 /meal 吃饭"), KeyboardButton(text="🚽 /pee 小便"), KeyboardButton(text="💩 /poop 大便")],
        [KeyboardButton(text="🚬 /smoke 抽烟"), KeyboardButton(text="↩️ /back 回来")],
        [KeyboardButton(text="📤 /export 导出(管理员)")],
    ],
    resize_keyboard=True
)

def now_sl() -> datetime:
    return datetime.now(tz=TZ)

def fmt_sl(dt: Optional[datetime]) -> str:
    if not dt:
        return ""
    return dt.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S")

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

async def safe_delete(chat_id: int, message_id: int):
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception:
        pass


# =========================
# 核心：按 07:00 / 19:00 分段归属
# 返回 (segment_date, segment_type)
# segment_date = 段起始所在日期
# DAY: 当天 07:00-19:00 -> segment_date=当天
# NIGHT: 当天 19:00-次日07:00 -> segment_date=当天
# 凌晨 00:00-06:59 -> 属于昨天的 NIGHT（segment_date=昨天）
# =========================
def segment_key(dt: datetime) -> Tuple[date, str]:
    local = dt.astimezone(TZ)
    h = local.hour
    if 7 <= h < 19:
        return local.date(), "DAY"
    if h >= 19:
        return local.date(), "NIGHT"
    # 00:00~06:59 -> 前一天 NIGHT
    return (local.date() - timedelta(days=1)), "NIGHT"


# =========================
# DB（新表名，避免旧库冲突）
# =========================
TABLE_SEG = "seg_summary_sl_7_19_v1"      # 每段汇总
TABLE_ACT = "active_session_sl_7_19_v1"  # 当前进行中（每人最多一条）
TABLE_EVT = "break_event_sl_7_19_v1"     # 每次明细


async def db_init():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)

    async with pool.acquire() as conn:
        # 每段汇总（段=DAY/NIGHT）
        await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_SEG} (
            chat_id BIGINT NOT NULL,
            tg_user_id BIGINT NOT NULL,
            tg_name TEXT,
            seg_date DATE NOT NULL,
            seg_type TEXT NOT NULL,              -- DAY / NIGHT

            pee_count INT NOT NULL DEFAULT 0,
            pee_min   INT NOT NULL DEFAULT 0,
            poop_count INT NOT NULL DEFAULT 0,
            poop_min   INT NOT NULL DEFAULT 0,
            meal_count INT NOT NULL DEFAULT 0,
            meal_min   INT NOT NULL DEFAULT 0,
            smoke_count INT NOT NULL DEFAULT 0,
            smoke_min   INT NOT NULL DEFAULT 0,

            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (chat_id, tg_user_id, seg_date, seg_type)
        );
        """)

        await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_seg_day_v1 ON {TABLE_SEG}(chat_id, seg_date);")

        # 进行中（开始时就锁定 seg_date/seg_type，避免跨段混乱）
        await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_ACT} (
            chat_id BIGINT NOT NULL,
            tg_user_id BIGINT NOT NULL,
            tg_name TEXT,

            seg_date DATE NOT NULL,
            seg_type TEXT NOT NULL,              -- DAY/NIGHT

            kind TEXT NOT NULL,                  -- pee/poop/meal/smoke
            start_at TIMESTAMPTZ NOT NULL,
            start_msg BIGINT,
            msg1 BIGINT,
            msg2 BIGINT,
            PRIMARY KEY (chat_id, tg_user_id)
        );
        """)

        # 明细
        await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_EVT} (
            id BIGSERIAL PRIMARY KEY,
            chat_id BIGINT NOT NULL,
            tg_user_id BIGINT NOT NULL,
            tg_name TEXT,

            seg_date DATE NOT NULL,
            seg_type TEXT NOT NULL,              -- DAY/NIGHT

            kind TEXT NOT NULL,
            start_at TIMESTAMPTZ NOT NULL,
            end_at   TIMESTAMPTZ NOT NULL,
            used_min INT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)
        await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_evt_seg_v1 ON {TABLE_EVT}(chat_id, seg_date, seg_type);")


# =========================
# DB 工具
# =========================
async def get_active(chat_id: int, tg_user_id: int):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            f"SELECT * FROM {TABLE_ACT} WHERE chat_id=$1 AND tg_user_id=$2",
            chat_id, tg_user_id
        )

async def set_active(chat_id: int, tg_user_id: int, tg_name: str, seg_date: date, seg_type: str,
                     kind: str, start_at: datetime, start_msg: int, msg1: int, msg2: int):
    async with pool.acquire() as conn:
        await conn.execute(
            f"""
            INSERT INTO {TABLE_ACT}(chat_id, tg_user_id, tg_name, seg_date, seg_type, kind, start_at, start_msg, msg1, msg2)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
            ON CONFLICT(chat_id, tg_user_id) DO UPDATE
            SET tg_name=EXCLUDED.tg_name,
                seg_date=EXCLUDED.seg_date,
                seg_type=EXCLUDED.seg_type,
                kind=EXCLUDED.kind,
                start_at=EXCLUDED.start_at,
                start_msg=EXCLUDED.start_msg,
                msg1=EXCLUDED.msg1,
                msg2=EXCLUDED.msg2
            """,
            chat_id, tg_user_id, tg_name, seg_date, seg_type, kind, start_at, start_msg, msg1, msg2
        )

async def clear_active(chat_id: int, tg_user_id: int):
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT * FROM {TABLE_ACT} WHERE chat_id=$1 AND tg_user_id=$2",
            chat_id, tg_user_id
        )
        await conn.execute(
            f"DELETE FROM {TABLE_ACT} WHERE chat_id=$1 AND tg_user_id=$2",
            chat_id, tg_user_id
        )
        return row

async def get_kind_count(chat_id: int, tg_user_id: int, seg_date: date, seg_type: str, kind: str) -> int:
    col = f"{kind}_count"
    async with pool.acquire() as conn:
        v = await conn.fetchval(
            f"""
            SELECT {col} FROM {TABLE_SEG}
            WHERE chat_id=$1 AND tg_user_id=$2 AND seg_date=$3 AND seg_type=$4
            """,
            chat_id, tg_user_id, seg_date, seg_type
        )
    return int(v or 0)

async def add_break_to_seg(chat_id: int, tg_user_id: int, tg_name: str, seg_date: date, seg_type: str, kind: str, used_min: int):
    count_col = f"{kind}_count"
    min_col = f"{kind}_min"
    async with pool.acquire() as conn:
        await conn.execute(
            f"""
            INSERT INTO {TABLE_SEG}(chat_id, tg_user_id, tg_name, seg_date, seg_type, {count_col}, {min_col})
            VALUES($1,$2,$3,$4,$5,1,$6)
            ON CONFLICT(chat_id, tg_user_id, seg_date, seg_type) DO UPDATE
            SET tg_name=EXCLUDED.tg_name,
                {count_col} = {TABLE_SEG}.{count_col} + 1,
                {min_col}   = {TABLE_SEG}.{min_col} + EXCLUDED.{min_col},
                updated_at=NOW()
            """,
            chat_id, tg_user_id, tg_name, seg_date, seg_type, used_min
        )

async def insert_break_event(chat_id: int, tg_user_id: int, tg_name: str, seg_date: date, seg_type: str,
                            kind: str, start_at: datetime, end_at: datetime, used_min: int):
    async with pool.acquire() as conn:
        await conn.execute(
            f"""
            INSERT INTO {TABLE_EVT}(chat_id, tg_user_id, tg_name, seg_date, seg_type, kind, start_at, end_at, used_min)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
            """,
            chat_id, tg_user_id, tg_name, seg_date, seg_type, kind, start_at, end_at, used_min
        )

async def fetch_export_segs(chat_id: int, start_day: date, end_day: date):
    async with pool.acquire() as conn:
        return await conn.fetch(
            f"""
            SELECT seg_date, seg_type, tg_user_id, tg_name,
                   pee_count, pee_min, poop_count, poop_min,
                   meal_count, meal_min, smoke_count, smoke_min
            FROM {TABLE_SEG}
            WHERE chat_id=$1 AND seg_date BETWEEN $2 AND $3
            ORDER BY seg_date ASC,
                     CASE WHEN seg_type='DAY' THEN 0 ELSE 1 END,
                     tg_user_id ASC
            """,
            chat_id, start_day, end_day
        )

async def fetch_export_events(chat_id: int, start_day: date, end_day: date):
    async with pool.acquire() as conn:
        return await conn.fetch(
            f"""
            SELECT seg_date, seg_type, tg_user_id, tg_name, kind, start_at, end_at, used_min
            FROM {TABLE_EVT}
            WHERE chat_id=$1 AND seg_date BETWEEN $2 AND $3
            ORDER BY seg_date ASC,
                     CASE WHEN seg_type='DAY' THEN 0 ELSE 1 END,
                     tg_user_id ASC, start_at ASC
            """,
            chat_id, start_day, end_day
        )


# =========================
# /start /export
# =========================
@dp.message(CommandStart())
async def start_cmd(message: Message):
    if message.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.reply(
            "✅ 休息计时机器人已启用（斯里兰卡时间 Asia/Colombo）\n\n"
            "刷新口径：每天两段统计\n"
            "白班段：07:00 ~ 19:00\n"
            "晚班段：19:00 ~ 次日07:00\n\n"
            "用法：\n"
            "/meal 吃饭 | /pee 小便 | /poop 大便 | /smoke 抽烟\n"
            "/back 回来（结束本次并结算）\n\n"
            "导出（仅管理员）：\n"
            "/export 2026-02-06\n"
            "/export 2026-02-01 2026-02-06\n",
            reply_markup=KB
        )
    else:
        await message.reply("请把机器人拉进群使用。", reply_markup=KB)


@dp.message(Command("export"))
async def export_cmd(message: Message):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return await message.reply("请在群里导出。")

    if not ADMIN_IDS:
        return await message.reply("未配置 ADMIN_IDS，当前禁止导出。请设置 ADMIN_IDS=xxx,yyy")
    if message.from_user.id not in ADMIN_IDS:
        return await message.reply("你没有导出权限。")

    parts = (message.text or "").split()
    today = now_sl().date()
    start_day = end_day = today

    def parse_d(s: str) -> Optional[date]:
        try:
            return datetime.strptime(s, "%Y-%m-%d").date()
        except Exception:
            return None

    if len(parts) == 2:
        d = parse_d(parts[1])
        if not d:
            return await message.reply("格式：/export 2026-02-06")
        start_day = end_day = d
    elif len(parts) >= 3:
        d1 = parse_d(parts[1])
        d2 = parse_d(parts[2])
        if not d1 or not d2:
            return await message.reply("格式：/export 2026-02-01 2026-02-06")
        start_day, end_day = (d1, d2) if d1 <= d2 else (d2, d1)

    wait = await message.reply("⏳ 正在导出请稍等…")
    try:
        segs = await fetch_export_segs(message.chat.id, start_day, end_day)
        events = await fetch_export_events(message.chat.id, start_day, end_day)

        # ===== 汇总 CSV =====
        buf1 = io.StringIO()
        w1 = csv.writer(buf1)
        w1.writerow([
            "日期(段起始日,斯里兰卡)",
            "班段",
            "用户ID",
            "用户名",
            "小便次数", "小便总分钟",
            "大便次数", "大便总分钟",
            "吃饭次数", "吃饭总分钟",
            "抽烟次数", "抽烟总分钟",
        ])

        for r in segs:
            uid_text = "\t" + str(int(r["tg_user_id"]))
            name_text = (r["tg_name"] or "").strip()
            seg_type = r["seg_type"]
            w1.writerow([
                str(r["seg_date"]),
                SEG_CN.get(seg_type, seg_type),
                uid_text,
                name_text,
                int(r["pee_count"]), int(r["pee_min"]),
                int(r["poop_count"]), int(r["poop_min"]),
                int(r["meal_count"]), int(r["meal_min"]),
                int(r["smoke_count"]), int(r["smoke_min"]),
            ])

        data1 = buf1.getvalue().encode("utf-8-sig")
        file1 = BufferedInputFile(data1, filename=f"休息汇总_两段_{message.chat.id}_{start_day}_{end_day}.csv")

        # ===== 明细 CSV =====
        buf2 = io.StringIO()
        w2 = csv.writer(buf2)
        w2.writerow([
            "日期(段起始日,斯里兰卡)",
            "班段",
            "用户ID",
            "用户名",
            "类型",
            "开始时间(斯里兰卡)",
            "结束时间(斯里兰卡)",
            "用时(分钟)",
        ])

        for e in events:
            uid_text = "\t" + str(int(e["tg_user_id"]))
            name_text = (e["tg_name"] or "").strip()
            seg_type = e["seg_type"]
            kind = e["kind"]
            kind_cn = KIND_CN.get(kind, kind)
            w2.writerow([
                str(e["seg_date"]),
                SEG_CN.get(seg_type, seg_type),
                uid_text,
                name_text,
                kind_cn,
                fmt_sl(e["start_at"]),
                fmt_sl(e["end_at"]),
                int(e["used_min"]),
            ])

        data2 = buf2.getvalue().encode("utf-8-sig")
        file2 = BufferedInputFile(data2, filename=f"休息明细_两段_{message.chat.id}_{start_day}_{end_day}.csv")

        await message.answer_document(file1)
        await message.answer_document(file2)
        await wait.edit_text("✅ 导出完成（已发送：汇总 + 明细）")
    except Exception as e:
        await wait.edit_text(f"❌ 导出失败：{type(e).__name__}: {e}")


# =========================
# 群消息统一入口
# =========================
@dp.message(F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}) & F.text)
async def on_group_text(message: Message):
    raw = (message.text or "").strip()
    raw_lower = raw.lower()

    m = re.search(r"/[a-z]+", raw_lower)
    cmd = m.group(0) if m else (raw_lower.split()[0] if raw_lower else "")

    kind = CMD_ALIASES.get(cmd)
    if not kind:
        kind = TEXT_ALIASES.get(raw) or TEXT_ALIASES.get(raw_lower)
    if not kind:
        return

    chat_id = message.chat.id
    tg_user_id = message.from_user.id
    tg_name = get_tg_name(message)
    mention = mention_html(message)
    now = now_sl()

    if kind == "export":
        message.text = "/export"
        return await export_cmd(message)

    active = await get_active(chat_id, tg_user_id)
    if active and kind != "back":
        return await message.reply("⚠️ 你当前还有进行中的状态，请先点【/back 回来】再继续。", reply_markup=KB)

    # 回来：按“开始时锁定的班段”结算，避免跨 07:00/19:00 混到新段
    if kind == "back":
        if not active:
            return await message.reply("你当前没有进行中的记录。", reply_markup=KB)

        act = await clear_active(chat_id, tg_user_id)
        start_at = act["start_at"]
        seg_date = act["seg_date"]
        seg_type = act["seg_type"]
        bk = act["kind"]

        used_min = int(max(0, (now - start_at).total_seconds() // 60))

        await insert_break_event(chat_id, tg_user_id, tg_name, seg_date, seg_type, bk, start_at, now, used_min)
        await add_break_to_seg(chat_id, tg_user_id, tg_name, seg_date, seg_type, bk, used_min)

        used_cnt = await get_kind_count(chat_id, tg_user_id, seg_date, seg_type, bk)
        limit = DAILY_LIMITS.get(bk, 999)
        left = max(0, limit - used_cnt)

        # 删除过程消息
        to_delete = []
        if act.get("start_msg"):
            to_delete.append(int(act["start_msg"]))
        if act.get("msg1"):
            to_delete.append(int(act["msg1"]))
        if act.get("msg2"):
            to_delete.append(int(act["msg2"]))
        to_delete.append(int(message.message_id))
        for mid in to_delete:
            await safe_delete(chat_id, mid)

        limit_min = DEFAULT_MINUTES.get(bk, 0)
        overtime = max(0, used_min - limit_min) if limit_min else 0
        extra = ""
        if limit_min:
            extra = f"\n⏱ 超时：{overtime} 分钟（提示 {limit_min} 分钟）" if overtime > 0 else f"\n✅ 未超时（提示 {limit_min} 分钟）"

        quote = random.choice(QUOTES_BACK)

        return await message.answer(
            f"✅ {mention} 已回来：本次【{KIND_CN.get(bk, bk)}】用时 {used_min} 分钟。"
            f"{extra}\n"
            f"📌 归属：{seg_date} - {SEG_CN.get(seg_type, seg_type)}\n"
            f"本段已用 {used_cnt}/{limit} 次，剩余 {left} 次。\n"
            f"{quote}",
            reply_markup=KB,
            parse_mode=ParseMode.HTML
        )

    # 开始休息：用当前时间计算所属段（07/19 自动切段）
    seg_date, seg_type = segment_key(now)

    used_cnt = await get_kind_count(chat_id, tg_user_id, seg_date, seg_type, kind)
    limit = DAILY_LIMITS.get(kind, 999)
    if used_cnt >= limit:
        return await message.reply(
            f"⛔️ 本段【{KIND_CN.get(kind, kind)}】次数已满：{used_cnt}/{limit}\n"
            f"📌 归属：{seg_date} - {SEG_CN.get(seg_type, seg_type)}",
            reply_markup=KB
        )

    minutes = DEFAULT_MINUTES.get(kind, 10)
    deadline = (now + timedelta(minutes=minutes)).astimezone(TZ).strftime("%H:%M")
    quote = random.choice(QUOTES_START)

    msg1 = await message.answer(
        f"📝 {mention} 已记录：{KIND_CN.get(kind, kind)}（第 {used_cnt + 1}/{limit} 次）\n"
        f"📌 归属：{seg_date} - {SEG_CN.get(seg_type, seg_type)}\n"
        f"{quote}",
        reply_markup=KB,
        parse_mode=ParseMode.HTML
    )
    msg2 = await message.answer(
        f"⏰ {mention} 请在 {deadline} 前回来（提示值 {minutes} 分钟）。\n"
        f"结束请点【/back 回来】",
        reply_markup=KB,
        parse_mode=ParseMode.HTML
    )

    await set_active(
        chat_id, tg_user_id, tg_name,
        seg_date, seg_type,
        kind, now,
        message.message_id,
        msg1.message_id,
        msg2.message_id
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
