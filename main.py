import os
import io
import re
import csv
import asyncio
import html
import random
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

TZ = ZoneInfo("Asia/Colombo")  # 斯里兰卡
DAY_CUT = time(7, 0)           # 07:00 作为“日界”
LATE_GRACE_MIN = 10            # 迟到宽限（分钟）

# ✅ 新表名：避免和你旧库冲突（关键）
T_SUM = "shift_summary_sl_v2"
T_ACT = "active_session_sl_v2"
T_PREF = "user_shift_pref_sl_v2"
T_USERS = "users_seen_sl_v2"


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
# 规则
# =========================
DAILY_LIMITS = {
    "pee": 3,
    "poop": 2,
    "meal": 3,
    "smoke": 5,
    "checkin": 1,
    "checkout": 1
}

DEFAULT_MINUTES = {
    "pee": 6,
    "poop": 15,
    "meal": 30,
    "smoke": 10,
}

KIND_CN = {
    "checkin": "上班",
    "checkout": "下班",
    "pee": "小便/厕所",
    "poop": "大便",
    "meal": "吃饭",
    "smoke": "抽烟",
}

CHECKIN_QUOTES = [
    "💪 开工！今天也要稳稳拿下。",
    "🔥 状态拉满，冲就完事了！",
    "✅ 打卡成功，保持节奏，慢慢赢。",
    "🚀 新的一班开始，专注就会有结果。",
    "🌟 加油！今天一定顺。",
]

CHECKOUT_QUOTES = [
    "👏 辛苦了！收工休息一下。",
    "✅ 下班啦，今天表现不错。",
    "🌙 结束一天，早点放松。",
    "💯 做得好，明天继续保持。",
]

# ✅ 命令别名（兼容 /xxx@botname）
CMD_ALIASES = {
    "/in": "checkin",
    "/out": "checkout",
    "/meal": "meal",
    "/pee": "pee",
    "/poop": "poop",
    "/smoke": "smoke",
    "/back": "back",
    "/export": "export",
    "/use": "use",
    "/who": "who",
    "/ping": "ping",
    "/missed": "missed",
}

TEXT_ALIASES = {
    "上班": "checkin",
    "下班": "checkout",
    "吃饭": "meal",
    "小便": "pee",
    "厕所": "pee",
    "大便": "poop",
    "抽烟": "smoke",
    "回来": "back",
    "回": "back",
    "back": "back",
    "导出": "export",
    "缺卡": "missed",
}

KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="/in 上班"), KeyboardButton(text="/out 下班")],
        [KeyboardButton(text="/meal 吃饭"), KeyboardButton(text="/pee 小便"), KeyboardButton(text="/poop 大便")],
        [KeyboardButton(text="/smoke 抽烟"), KeyboardButton(text="/back 回来")],
        [KeyboardButton(text="/export 导出"), KeyboardButton(text="/missed 缺卡")],
    ],
    resize_keyboard=True
)


# =========================
# 时间口径：07:00 切日 + 白夜班
# =========================
def now_sl() -> datetime:
    return datetime.now(tz=TZ)


def work_day(dt: datetime) -> date:
    lt = dt.astimezone(TZ)
    if lt.time() >= DAY_CUT:
        return lt.date()
    return lt.date() - timedelta(days=1)


def infer_shift(dt: datetime) -> str:
    lt = dt.astimezone(TZ)
    t = lt.time()
    if time(7, 0) <= t < time(19, 0):
        return "白班"
    return "夜班"


def shift_start_dt(wd: date, shift: str) -> datetime:
    if shift == "白班":
        return datetime.combine(wd, time(7, 0), TZ)
    return datetime.combine(wd, time(19, 0), TZ)


def is_on_time(checkin_at: datetime, wd: date, shift: str) -> bool:
    start = shift_start_dt(wd, shift)
    return checkin_at <= (start + timedelta(minutes=LATE_GRACE_MIN))


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


def extract_cmd(text: str) -> str:
    # 兼容：/pee@YourBot
    m = re.match(r"^/([a-zA-Z_]+)(?:@[\w_]+)?", (text or "").strip())
    return f"/{m.group(1).lower()}" if m else ""


# =========================
# DB 初始化（新表 v2）
# =========================
async def db_init():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    async with pool.acquire() as conn:
        await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {T_USERS} (
            chat_id BIGINT NOT NULL,
            tg_user_id BIGINT NOT NULL,
            tg_name TEXT,
            first_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_seen  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY(chat_id, tg_user_id)
        );
        """)

        await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {T_SUM} (
            chat_id BIGINT NOT NULL,
            tg_user_id BIGINT NOT NULL,
            tg_name TEXT,
            work_day DATE NOT NULL,
            shift TEXT NOT NULL,

            checkin_at TIMESTAMPTZ,
            on_time BOOLEAN,
            checkout_at TIMESTAMPTZ,

            pee_count INT NOT NULL DEFAULT 0,
            pee_min   INT NOT NULL DEFAULT 0,
            poop_count INT NOT NULL DEFAULT 0,
            poop_min   INT NOT NULL DEFAULT 0,
            meal_count INT NOT NULL DEFAULT 0,
            meal_min   INT NOT NULL DEFAULT 0,
            smoke_count INT NOT NULL DEFAULT 0,
            smoke_min   INT NOT NULL DEFAULT 0,

            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (chat_id, tg_user_id, work_day, shift)
        );
        """)

        await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {T_ACT} (
            chat_id BIGINT NOT NULL,
            tg_user_id BIGINT NOT NULL,
            work_day DATE NOT NULL,
            shift TEXT NOT NULL,
            kind TEXT NOT NULL,
            start_at TIMESTAMPTZ NOT NULL,
            start_msg BIGINT,
            msg1 BIGINT,
            msg2 BIGINT,
            PRIMARY KEY (chat_id, tg_user_id)
        );
        """)

        await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {T_PREF} (
            chat_id BIGINT NOT NULL,
            tg_user_id BIGINT NOT NULL,
            shift TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (chat_id, tg_user_id)
        );
        """)

        await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_sum_day_shift_v2 ON {T_SUM}(chat_id, work_day, shift);")


# =========================
# DB 工具
# =========================
async def touch_user(chat_id: int, tg_user_id: int, tg_name: str):
    async with pool.acquire() as conn:
        await conn.execute(f"""
        INSERT INTO {T_USERS}(chat_id, tg_user_id, tg_name)
        VALUES($1,$2,$3)
        ON CONFLICT(chat_id, tg_user_id) DO UPDATE
        SET tg_name=EXCLUDED.tg_name, last_seen=NOW()
        """, chat_id, tg_user_id, tg_name)


async def get_user_shift(chat_id: int, tg_user_id: int, dt: datetime) -> str:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT shift FROM {T_PREF} WHERE chat_id=$1 AND tg_user_id=$2",
            chat_id, tg_user_id
        )
    if row and row["shift"] in ("白班", "夜班"):
        return row["shift"]
    return infer_shift(dt)


async def set_user_shift(chat_id: int, tg_user_id: int, shift: str):
    async with pool.acquire() as conn:
        await conn.execute(f"""
        INSERT INTO {T_PREF}(chat_id, tg_user_id, shift)
        VALUES($1,$2,$3)
        ON CONFLICT(chat_id, tg_user_id) DO UPDATE
        SET shift=EXCLUDED.shift, updated_at=NOW()
        """, chat_id, tg_user_id, shift)


async def ensure_summary_row(chat_id: int, tg_user_id: int, tg_name: str, wd: date, shift: str):
    async with pool.acquire() as conn:
        await conn.execute(f"""
        INSERT INTO {T_SUM}(chat_id, tg_user_id, tg_name, work_day, shift)
        VALUES($1,$2,$3,$4,$5)
        ON CONFLICT(chat_id, tg_user_id, work_day, shift)
        DO UPDATE SET tg_name=EXCLUDED.tg_name, updated_at=NOW()
        """, chat_id, tg_user_id, tg_name, wd, shift)


async def get_active(chat_id: int, tg_user_id: int):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            f"SELECT * FROM {T_ACT} WHERE chat_id=$1 AND tg_user_id=$2",
            chat_id, tg_user_id
        )


async def set_active(chat_id: int, tg_user_id: int, wd: date, shift: str, kind: str,
                     start_at: datetime, start_msg: int, msg1: int, msg2: int):
    async with pool.acquire() as conn:
        await conn.execute(f"""
        INSERT INTO {T_ACT}(chat_id, tg_user_id, work_day, shift, kind, start_at, start_msg, msg1, msg2)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
        ON CONFLICT(chat_id, tg_user_id) DO UPDATE
        SET work_day=EXCLUDED.work_day,
            shift=EXCLUDED.shift,
            kind=EXCLUDED.kind,
            start_at=EXCLUDED.start_at,
            start_msg=EXCLUDED.start_msg,
            msg1=EXCLUDED.msg1,
            msg2=EXCLUDED.msg2
        """, chat_id, tg_user_id, wd, shift, kind, start_at, start_msg, msg1, msg2)


async def clear_active(chat_id: int, tg_user_id: int):
    async with pool.acquire() as conn:
        row = await conn.fetchrow(f"SELECT * FROM {T_ACT} WHERE chat_id=$1 AND tg_user_id=$2", chat_id, tg_user_id)
        await conn.execute(f"DELETE FROM {T_ACT} WHERE chat_id=$1 AND tg_user_id=$2", chat_id, tg_user_id)
        return row


async def get_checkin(chat_id: int, tg_user_id: int, wd: date, shift: str) -> Optional[datetime]:
    async with pool.acquire() as conn:
        return await conn.fetchval(f"""
        SELECT checkin_at FROM {T_SUM}
        WHERE chat_id=$1 AND tg_user_id=$2 AND work_day=$3 AND shift=$4
        """, chat_id, tg_user_id, wd, shift)


async def get_checkout(chat_id: int, tg_user_id: int, wd: date, shift: str) -> Optional[datetime]:
    async with pool.acquire() as conn:
        return await conn.fetchval(f"""
        SELECT checkout_at FROM {T_SUM}
        WHERE chat_id=$1 AND tg_user_id=$2 AND work_day=$3 AND shift=$4
        """, chat_id, tg_user_id, wd, shift)


async def add_checkin(chat_id: int, tg_user_id: int, wd: date, shift: str, checkin_at: datetime):
    ot = is_on_time(checkin_at, wd, shift)
    async with pool.acquire() as conn:
        await conn.execute(f"""
        UPDATE {T_SUM}
        SET checkin_at=$1, on_time=$2, updated_at=NOW()
        WHERE chat_id=$3 AND tg_user_id=$4 AND work_day=$5 AND shift=$6
        """, checkin_at, ot, chat_id, tg_user_id, wd, shift)


async def add_checkout(chat_id: int, tg_user_id: int, wd: date, shift: str, checkout_at: datetime):
    async with pool.acquire() as conn:
        await conn.execute(f"""
        UPDATE {T_SUM}
        SET checkout_at=$1, updated_at=NOW()
        WHERE chat_id=$2 AND tg_user_id=$3 AND work_day=$4 AND shift=$5
        """, checkout_at, chat_id, tg_user_id, wd, shift)


async def get_kind_count(chat_id: int, tg_user_id: int, wd: date, shift: str, kind: str) -> int:
    col = f"{kind}_count"
    async with pool.acquire() as conn:
        v = await conn.fetchval(f"""
        SELECT {col} FROM {T_SUM}
        WHERE chat_id=$1 AND tg_user_id=$2 AND work_day=$3 AND shift=$4
        """, chat_id, tg_user_id, wd, shift)
    return int(v or 0)


async def add_break_result(chat_id: int, tg_user_id: int, wd: date, shift: str, kind: str, used_min: int):
    count_col = f"{kind}_count"
    min_col = f"{kind}_min"
    async with pool.acquire() as conn:
        await conn.execute(f"""
        UPDATE {T_SUM}
        SET {count_col} = {count_col} + 1,
            {min_col}   = {min_col} + $1,
            updated_at = NOW()
        WHERE chat_id=$2 AND tg_user_id=$3 AND work_day=$4 AND shift=$5
        """, used_min, chat_id, tg_user_id, wd, shift)


async def fetch_export(chat_id: int, d: date):
    async with pool.acquire() as conn:
        return await conn.fetch(f"""
        SELECT work_day, shift, tg_user_id, tg_name, checkin_at, on_time, checkout_at,
               pee_count, pee_min, poop_count, poop_min,
               meal_count, meal_min, smoke_count, smoke_min
        FROM {T_SUM}
        WHERE chat_id=$1 AND work_day=$2
        ORDER BY shift ASC, tg_user_id ASC
        """, chat_id, d)


async def fetch_seen_users(chat_id: int):
    async with pool.acquire() as conn:
        return await conn.fetch(f"SELECT tg_user_id, tg_name FROM {T_USERS} WHERE chat_id=$1 ORDER BY tg_user_id ASC", chat_id)


async def fetch_present_users(chat_id: int, d: date):
    async with pool.acquire() as conn:
        return await conn.fetch(f"""
        SELECT DISTINCT tg_user_id, shift FROM {T_SUM}
        WHERE chat_id=$1 AND work_day=$2
        """, chat_id, d)


# =========================
# 指令
# =========================
@dp.message(CommandStart())
async def start_cmd(message: Message):
    await message.reply(
        "✅ 打卡机器人已启用（斯里兰卡时间）\n\n"
        "上/下班：/in  /out\n"
        "休息：/meal /pee /poop /smoke\n"
        "结束：/back\n"
        "切班（共用号）：/use 白班 或 /use 夜班\n"
        "导出（管理员）：/export 2026-02-18\n"
        "缺卡（管理员）：/missed 2026-02-18",
        reply_markup=KB
    )


@dp.message(Command("ping"))
async def ping_cmd(message: Message):
    await message.reply("pong ✅ 我收到消息了")


@dp.message(Command("use"))
async def use_cmd(message: Message):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return await message.reply("请在群里使用 /use。")
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        return await message.reply("用法：/use 白班  或  /use 夜班")
    shift = parts[1].strip()
    if shift not in ("白班", "夜班"):
        return await message.reply("只支持：白班 / 夜班")
    await set_user_shift(message.chat.id, message.from_user.id, shift)
    await message.reply(f"✅ 当前班次已切换为：{shift}", reply_markup=KB)


@dp.message(Command("who"))
async def who_cmd(message: Message):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return await message.reply("请在群里使用 /who。")
    shift = await get_user_shift(message.chat.id, message.from_user.id, now_sl())
    wd = work_day(now_sl())
    await message.reply(f"当前：{shift} | 工作日：{wd}（07:00~次日07:00）", reply_markup=KB)


@dp.message(Command("export"))
async def export_cmd(message: Message):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return await message.reply("请在群里导出。")
    if not ADMIN_IDS or (message.from_user.id not in ADMIN_IDS):
        return await message.reply("你没有导出权限。")

    parts = (message.text or "").split()
    if len(parts) < 2:
        return await message.reply("格式：/export 2026-02-18")

    try:
        d = datetime.strptime(parts[1], "%Y-%m-%d").date()
    except Exception:
        return await message.reply("格式：/export 2026-02-18")

    wait = await message.reply("⏳ 正在导出请稍等…")
    try:
        rows = await fetch_export(message.chat.id, d)

        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow([
            "工作日(07:00~次日07:00)",
            "班次",
            "用户ID",
            "用户名",
            "上班时间(斯里兰卡)",
            "是否按时",
            "下班时间(斯里兰卡)",
            "小便次数", "小便总分钟",
            "大便次数", "大便总分钟",
            "吃饭次数", "吃饭总分钟",
            "抽烟次数", "抽烟总分钟",
        ])

        for r in rows:
            uid_text = "\t" + str(int(r["tg_user_id"]))
            name_text = (r["tg_name"] or "").strip()
            ci = r["checkin_at"]
            co = r["checkout_at"]
            ci_txt = ci.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S") if ci else ""
            co_txt = co.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S") if co else ""
            ot_txt = "是" if r["on_time"] else ("否" if ci else "未打卡")

            w.writerow([
                str(r["work_day"]),
                r["shift"],
                uid_text,
                name_text,
                ci_txt,
                ot_txt,
                co_txt,
                int(r["pee_count"]), int(r["pee_min"]),
                int(r["poop_count"]), int(r["poop_min"]),
                int(r["meal_count"]), int(r["meal_min"]),
                int(r["smoke_count"]), int(r["smoke_min"]),
            ])

        data = buf.getvalue().encode("utf-8-sig")
        filename = f"打卡汇总_{message.chat.id}_{d}.csv"
        await message.answer_document(BufferedInputFile(data, filename=filename))
        await wait.edit_text("✅ 导出完成")
    except Exception as e:
        await wait.edit_text(f"❌ 导出失败：{type(e).__name__}: {e}")


@dp.message(Command("missed"))
async def missed_cmd(message: Message):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return await message.reply("请在群里使用。")
    if not ADMIN_IDS or (message.from_user.id not in ADMIN_IDS):
        return await message.reply("你没有权限。")

    parts = (message.text or "").split()
    if len(parts) < 2:
        return await message.reply("格式：/missed 2026-02-18")

    try:
        d = datetime.strptime(parts[1], "%Y-%m-%d").date()
    except Exception:
        return await message.reply("格式：/missed 2026-02-18")

    seen = await fetch_seen_users(message.chat.id)
    present = await fetch_present_users(message.chat.id, d)

    all_seen = [(int(r["tg_user_id"]), (r["tg_name"] or "").strip()) for r in seen]
    present_day = set()
    present_shift = {"白班": set(), "夜班": set()}
    for r in present:
        uid = int(r["tg_user_id"])
        sh = r["shift"]
        present_day.add(uid)
        if sh in present_shift:
            present_shift[sh].add(uid)

    def fmt(lst):
        if not lst:
            return "（无）"
        return "\n".join([f"- {name or uid} ({uid})" for uid, name in lst])

    missed_all = [(uid, name) for uid, name in all_seen if uid not in present_day]
    missed_day = [(uid, name) for uid, name in all_seen if uid not in present_shift["白班"]]
    missed_night = [(uid, name) for uid, name in all_seen if uid not in present_shift["夜班"]]

    await message.reply(
        f"🧾 缺卡统计（{d}）\n\n"
        f"❌ 一天都没打卡：\n{fmt(missed_all)}\n\n"
        f"❌ 白班没打卡：\n{fmt(missed_day)}\n\n"
        f"❌ 夜班没打卡：\n{fmt(missed_night)}",
        reply_markup=KB
    )


# =========================
# 群消息统一入口
# =========================
@dp.message(F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}) & F.text)
async def on_group_text(message: Message):
    raw = (message.text or "").strip()
    cmd = extract_cmd(raw)

    # 登记出现过的账号（用于缺卡）
    if message.from_user:
        await touch_user(message.chat.id, message.from_user.id, get_tg_name(message))

    kind = CMD_ALIASES.get(cmd)
    if not kind:
        kind = TEXT_ALIASES.get(raw) or TEXT_ALIASES.get(raw.lower())
    if not kind:
        return

    chat_id = message.chat.id
    tg_user_id = message.from_user.id
    tg_name = get_tg_name(message)
    mention = mention_html(message)

    now = now_sl()
    wd = work_day(now)
    shift = await get_user_shift(chat_id, tg_user_id, now)

    await ensure_summary_row(chat_id, tg_user_id, tg_name, wd, shift)

    active = await get_active(chat_id, tg_user_id)
    if active and kind not in ("back",):
        return await message.reply("⚠️ 你当前还有进行中的状态，请先 /back 再继续。", reply_markup=KB)

    # back：结算
    if kind == "back":
        if not active:
            return await message.reply("没有进行中的记录。", reply_markup=KB)

        act = await clear_active(chat_id, tg_user_id)
        used_min = int(max(0, (now - act["start_at"]).total_seconds() // 60))
        bk = act["kind"]
        act_wd = act["work_day"]
        act_shift = act["shift"]

        await ensure_summary_row(chat_id, tg_user_id, tg_name, act_wd, act_shift)
        await add_break_result(chat_id, tg_user_id, act_wd, act_shift, bk, used_min)

        # 删过程消息
        for mid in [act.get("start_msg"), act.get("msg1"), act.get("msg2"), message.message_id]:
            if mid:
                await safe_delete(chat_id, int(mid))

        return await message.answer(
            f"✅ {mention} 已回来：本次【{KIND_CN.get(bk, bk)}】用时 {used_min} 分钟。\n"
            f"归属：{act_wd} {act_shift}",
            reply_markup=KB,
            parse_mode=ParseMode.HTML
        )

    # 上班
    if kind == "checkin":
        exist = await get_checkin(chat_id, tg_user_id, wd, shift)
        if exist:
            return await message.reply("⛔️ 本班次已经打过上班了。", reply_markup=KB)
        await add_checkin(chat_id, tg_user_id, wd, shift, now)
        quote = random.choice(CHECKIN_QUOTES)
        ot = is_on_time(now, wd, shift)
        return await message.answer(
            f"✅ {mention} 上班打卡成功（{wd} {shift}）\n"
            f"{'✅ 按时' if ot else '⚠️ 已迟到'}（宽限 {LATE_GRACE_MIN} 分钟）\n"
            f"{quote}",
            reply_markup=KB,
            parse_mode=ParseMode.HTML
        )

    # 下班
    if kind == "checkout":
        ci = await get_checkin(chat_id, tg_user_id, wd, shift)
        if not ci:
            return await message.reply("⛔️ 你还没上班，不能下班。请先 /in", reply_markup=KB)
        co = await get_checkout(chat_id, tg_user_id, wd, shift)
        if co:
            return await message.reply("⛔️ 本班次已经打过下班了。", reply_markup=KB)
        await add_checkout(chat_id, tg_user_id, wd, shift, now)
        used = int(max(0, (now - ci).total_seconds() // 60))
        quote = random.choice(CHECKOUT_QUOTES)
        return await message.answer(
            f"✅ {mention} 下班打卡成功（{wd} {shift}）\n"
            f"⏱ 在岗：{used} 分钟（{used/60:.2f} 小时）\n"
            f"{quote}",
            reply_markup=KB,
            parse_mode=ParseMode.HTML
        )

    # 休息：必须先上班（按班次）
    ci = await get_checkin(chat_id, tg_user_id, wd, shift)
    if not ci:
        return await message.reply("⛔️ 你还没上班，不能休息。请先 /in", reply_markup=KB)

    # 次数限制
    used_cnt = await get_kind_count(chat_id, tg_user_id, wd, shift, kind)
    limit = DAILY_LIMITS.get(kind, 999)
    if used_cnt >= limit:
        return await message.reply(f"⛔️ 今日（{wd} {shift}）【{KIND_CN.get(kind, kind)}】次数已满。", reply_markup=KB)

    minutes = DEFAULT_MINUTES.get(kind, 10)
    deadline = (now + timedelta(minutes=minutes)).astimezone(TZ).strftime("%H:%M")

    msg1 = await message.answer(
        f"📝 {mention} 已记录：{KIND_CN.get(kind, kind)}（第 {used_cnt + 1}/{limit} 次）\n归属：{wd} {shift}",
        reply_markup=KB,
        parse_mode=ParseMode.HTML
    )
    msg2 = await message.answer(
        f"⏰ {mention} 请在 {deadline} 前回来（提示值 {minutes} 分钟）。\n结束请发 /back",
        reply_markup=KB,
        parse_mode=ParseMode.HTML
    )

    await set_active(chat_id, tg_user_id, wd, shift, kind, now, message.message_id, msg1.message_id, msg2.message_id)


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
