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

TZ = ZoneInfo("Asia/Colombo")  # 斯里兰卡时间
DAY_CUT = time(7, 0)           # 07:00 切日

# ✅ 新表名：避免和你之前旧表冲突（很关键）
T_SUM = "shift_summary_sl_v3"
T_ACT = "active_session_sl_v3"
T_PREF = "user_shift_pref_sl_v3"
T_USERS = "users_seen_sl_v3"


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

TEXT_ALIASES = {
    "吃饭": "meal",
    "小便": "pee",
    "厕所": "pee",
    "大便": "poop",
    "抽烟": "smoke",
    "回来": "back",
    "回": "back",
    "back": "back",
}

KB = ReplyKeyboardMarkup(
    keyboard=[
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
    """07:00~次日07:00 算同一天"""
    lt = dt.astimezone(TZ)
    if lt.time() >= DAY_CUT:
        return lt.date()
    return lt.date() - timedelta(days=1)


def infer_shift(dt: datetime) -> str:
    """07:00-19:00 白班；19:00-次日07:00 夜班"""
    lt = dt.astimezone(TZ)
    t = lt.time()
    if time(7, 0) <= t < time(19, 0):
        return "白班"
    return "夜班"


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


def parse_date_ymd(s: str) -> Optional[date]:
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


# =========================
# DB 初始化
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
            kind TEXT NOT NULL,              -- pee/poop/meal/smoke
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
            shift TEXT NOT NULL,             -- 白班/夜班
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (chat_id, tg_user_id)
        );
        """)

        await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_sum_day_shift_v3 ON {T_SUM}(chat_id, work_day, shift);")


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


async def fetch_export(chat_id: int, start_day: date, end_day: date, shift: Optional[str] = None):
    async with pool.acquire() as conn:
        if shift in ("白班", "夜班"):
            return await conn.fetch(f"""
            SELECT work_day, shift, tg_user_id, tg_name,
                   pee_count, pee_min, poop_count, poop_min,
                   meal_count, meal_min, smoke_count, smoke_min
            FROM {T_SUM}
            WHERE chat_id=$1 AND work_day BETWEEN $2 AND $3 AND shift=$4
            ORDER BY work_day ASC, tg_user_id ASC
            """, chat_id, start_day, end_day, shift)

        return await conn.fetch(f"""
        SELECT work_day, shift, tg_user_id, tg_name,
               pee_count, pee_min, poop_count, poop_min,
               meal_count, meal_min, smoke_count, smoke_min
        FROM {T_SUM}
        WHERE chat_id=$1 AND work_day BETWEEN $2 AND $3
        ORDER BY work_day ASC, shift ASC, tg_user_id ASC
        """, chat_id, start_day, end_day)


async def fetch_seen_users(chat_id: int):
    async with pool.acquire() as conn:
        return await conn.fetch(
            f"SELECT tg_user_id, tg_name FROM {T_USERS} WHERE chat_id=$1 ORDER BY tg_user_id ASC",
            chat_id
        )


async def fetch_present_users(chat_id: int, d: date):
    async with pool.acquire() as conn:
        return await conn.fetch(f"""
        SELECT tg_user_id, shift,
               (pee_count + poop_count + meal_count + smoke_count) AS total_cnt
        FROM {T_SUM}
        WHERE chat_id=$1 AND work_day=$2
        """, chat_id, d)


# =========================
# 核心处理：统一逻辑（命令与文本都走这里）
# =========================
async def handle_kind_message(message: Message, kind: str):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return

    chat_id = message.chat.id
    tg_user_id = message.from_user.id
    tg_name = get_tg_name(message)
    mention = mention_html(message)

    await touch_user(chat_id, tg_user_id, tg_name)

    now = now_sl()
    wd = work_day(now)
    shift = await get_user_shift(chat_id, tg_user_id, now)

    await ensure_summary_row(chat_id, tg_user_id, tg_name, wd, shift)

    active = await get_active(chat_id, tg_user_id)

    # 有进行中：只能 back
    if active and kind != "back":
        return await message.reply("⚠️ 你当前还有进行中的状态，请先回来 /back 再继续。", reply_markup=KB)

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

        # 删除过程消息
        for mid in [act.get("start_msg"), act.get("msg1"), act.get("msg2"), message.message_id]:
            if mid:
                await safe_delete(chat_id, int(mid))

        limit_min = DEFAULT_MINUTES.get(bk, 0)
        overtime = max(0, used_min - limit_min) if limit_min else 0
        extra = f"（超时 {overtime} 分钟）" if overtime > 0 else "（未超时）"

        return await message.answer(
            f"✅ {mention} 已回来：本次【{KIND_CN.get(bk, bk)}】用时 {used_min} 分钟 {extra}\n"
            f"归属：{act_wd} {act_shift}",
            reply_markup=KB,
            parse_mode=ParseMode.HTML
        )

    # 次数限制
    used_cnt = await get_kind_count(chat_id, tg_user_id, wd, shift, kind)
    limit = DAILY_LIMITS.get(kind, 999)
    if used_cnt >= limit:
        return await message.reply(
            f"⛔️ 今日（{wd} {shift}）【{KIND_CN.get(kind, kind)}】次数已满,如有急事请找领班申请次数：{used_cnt}/{limit}。",
            reply_markup=KB 
        )

    minutes = DEFAULT_MINUTES.get(kind, 10)
    deadline = (now + timedelta(minutes=minutes)).astimezone(TZ).strftime("%H:%M")

    msg1 = await message.answer(
        f"📝 {mention} 已记录：{KIND_CN.get(kind, kind)}（第 {used_cnt + 1}/{limit} 次）\n"
        f"归属：{wd} {shift}",
        reply_markup=KB,
        parse_mode=ParseMode.HTML
    )
    msg2 = await message.answer(
        f"⏰ {mention} 请在 {deadline} 前回来（提示值 {minutes} 分钟）。\n"
        f"结束请发 /back",
        reply_markup=KB,
        parse_mode=ParseMode.HTML
    )

    await set_active(chat_id, tg_user_id, wd, shift, kind, now, message.message_id, msg1.message_id, msg2.message_id)


# =========================
# 指令
# =========================
@dp.message(CommandStart())
async def start_cmd(message: Message):
    await message.reply(
        "✅ 打卡机器人已启用（斯里兰卡时间）\n\n"
        "开始：/meal /pee /poop /smoke\n"
        "结束：/back（结算本次用时）\n"
        "共用号切班：/use 白班 或 /use 夜班（不设就按时间自动判班）\n\n"
        "导出（管理员）：\n"
        "/export 2026-02-18\n"
        "/export 2026-02-18 2026-02-21\n"
        "/export 2026-02-18 2026-02-21 白班|夜班\n\n"
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
    now = now_sl()
    shift = await get_user_shift(message.chat.id, message.from_user.id, now)
    wd = work_day(now)
    await message.reply(f"当前：{shift} | 工作日：{wd}（07:00~次日07:00）", reply_markup=KB)


# ✅ 命令：休息开始/结束（关键：不再依赖 F.text）
@dp.message(Command("meal"))
async def cmd_meal(message: Message):
    return await handle_kind_message(message, "meal")


@dp.message(Command("pee"))
async def cmd_pee(message: Message):
    return await handle_kind_message(message, "pee")


@dp.message(Command("poop"))
async def cmd_poop(message: Message):
    return await handle_kind_message(message, "poop")


@dp.message(Command("smoke"))
async def cmd_smoke(message: Message):
    return await handle_kind_message(message, "smoke")


@dp.message(Command("back"))
async def cmd_back(message: Message):
    return await handle_kind_message(message, "back")


@dp.message(Command("export"))
async def export_cmd(message: Message):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return await message.reply("请在群里导出。")
    if not ADMIN_IDS or (message.from_user.id not in ADMIN_IDS):
        return await message.reply("你没有导出权限。")

    parts = (message.text or "").split()
    if len(parts) < 2:
        return await message.reply(
            "格式：\n"
            "/export 2026-02-18\n"
            "/export 2026-02-18 2026-02-21\n"
            "/export 2026-02-18 2026-02-21 白班|夜班"
        )

    d1 = parse_date_ymd(parts[1])
    if not d1:
        return await message.reply("日期格式错误：请用 YYYY-MM-DD")

    start_day = end_day = d1
    want_shift = None

    if len(parts) >= 3:
        d2 = parse_date_ymd(parts[2])
        if d2:
            start_day, end_day = (d1, d2) if d1 <= d2 else (d2, d1)
            if len(parts) >= 4 and parts[3] in ("白班", "夜班"):
                want_shift = parts[3]
        else:
            if parts[2] in ("白班", "夜班"):
                want_shift = parts[2]
            else:
                return await message.reply("格式错误：/export 2026-02-18 2026-02-21  或  /export 2026-02-18 白班")

    wait = await message.reply("⏳ 正在导出请稍等…")
    try:
        rows = await fetch_export(message.chat.id, start_day, end_day, want_shift)

        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow([
            "工作日(07:00~次日07:00)",
            "班次",
            "用户ID",
            "用户名",
            "小便次数", "小便总分钟",
            "大便次数", "大便总分钟",
            "吃饭次数", "吃饭总分钟",
            "抽烟次数", "抽烟总分钟",
        ])

        for r in rows:
            uid_text = "\t" + str(int(r["tg_user_id"]))
            name_text = (r["tg_name"] or "").strip()
            w.writerow([
                str(r["work_day"]),
                r["shift"],
                uid_text,
                name_text,
                int(r["pee_count"]), int(r["pee_min"]),
                int(r["poop_count"]), int(r["poop_min"]),
                int(r["meal_count"]), int(r["meal_min"]),
                int(r["smoke_count"]), int(r["smoke_min"]),
            ])

        data = buf.getvalue().encode("utf-8-sig")
        suffix = want_shift if want_shift else "全部"
        filename = f"打卡汇总_{message.chat.id}_{start_day}_{end_day}_{suffix}.csv"
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

    d = parse_date_ymd(parts[1])
    if not d:
        return await message.reply("日期格式错误：/missed 2026-02-18")

    seen = await fetch_seen_users(message.chat.id)
    present_rows = await fetch_present_users(message.chat.id, d)

    all_seen = [(int(r["tg_user_id"]), (r["tg_name"] or "").strip()) for r in seen]

    present_day = set()
    present_white = set()
    present_night = set()

    for r in present_rows:
        uid = int(r["tg_user_id"])
        sh = r["shift"]
        total_cnt = int(r["total_cnt"] or 0)
        if total_cnt > 0:
            present_day.add(uid)
            if sh == "白班":
                present_white.add(uid)
            elif sh == "夜班":
                present_night.add(uid)

    def fmt(lst):
        if not lst:
            return "（无）"
        return "\n".join([f"- {name or uid} ({uid})" for uid, name in lst])

    missed_all = [(uid, name) for uid, name in all_seen if uid not in present_day]
    missed_white = [(uid, name) for uid, name in all_seen if uid not in present_white]
    missed_night = [(uid, name) for uid, name in all_seen if uid not in present_night]

    await message.reply(
        f"🧾 缺卡统计（{d}）\n\n"
        f"❌ 一天都没任何记录：\n{fmt(missed_all)}\n\n"
        f"❌ 白班没任何记录：\n{fmt(missed_white)}\n\n"
        f"❌ 夜班没任何记录：\n{fmt(missed_night)}",
        reply_markup=KB
    )


# =========================
# 兼容：纯文字（可选）
# =========================
@dp.message(F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}) & F.text)
async def on_group_text(message: Message):
    raw = (message.text or "").strip()

    # 如果是 /命令，交给 Command handlers（避免重复）
    if raw.startswith("/"):
        return

    kind = TEXT_ALIASES.get(raw) or TEXT_ALIASES.get(raw.lower())
    if not kind:
        return

    if kind == "back":
        return await handle_kind_message(message, "back")
    return await handle_kind_message(message, kind)


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
