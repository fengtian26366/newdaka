import os
import io
import re
import csv
import asyncio
import html
import signal
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

# ✅ 斯里兰卡时间
TZ = ZoneInfo("Asia/Colombo")

# 班次分界
DAY_START = time(7, 0)     # 07:00
NIGHT_START = time(19, 0)  # 19:00


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
# 规则：只记录“休息类打卡”（按账号）
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

# ✅ 命令（隐私模式也能收到）
CMD_ALIASES = {
    "/meal": "meal",
    "/pee": "pee",
    "/poop": "poop",
    "/smoke": "smoke",
    "/back": "back",
    "/export": "export",
    "/missed": "missed",
}

# 可选：纯文字（隐私模式开着可能收不到）
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
    "缺卡": "missed",
}

# ⚠️ 按钮文字必须以 / 开头（避免 Telegram 不当命令转发的问题）
KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="/meal 吃饭"), KeyboardButton(text="/pee 小便"), KeyboardButton(text="/poop 大便")],
        [KeyboardButton(text="/smoke 抽烟"), KeyboardButton(text="/back 回来")],
        [KeyboardButton(text="/export 导出"), KeyboardButton(text="/missed 缺卡")],
    ],
    resize_keyboard=True
)


# =========================
# 时间：斯里兰卡 + 白班/夜班 + “班次日期”
# - 白班(07:00-18:59)：shift_date = 当天
# - 夜班(19:00-23:59)：shift_date = 当天
# - 夜班(00:00-06:59)：shift_date = 前一天（属于前一天夜班）
# =========================
def now_sl() -> datetime:
    return datetime.now(tz=TZ)


def infer_shift_and_date(dt: datetime) -> tuple[str, date]:
    lt = dt.astimezone(TZ)
    t = lt.time()
    if DAY_START <= t < NIGHT_START:
        return "白班", lt.date()
    if t < DAY_START:
        return "夜班", (lt.date() - timedelta(days=1))
    return "夜班", lt.date()


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
# DB 表
# =========================
T_USERS = "users_seen_sl_v1"
T_SUM = "shift_summary_sl_v1"
T_ACT = "active_session_sl_v1"
T_EVT = "break_event_sl_v1"


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
            PRIMARY KEY (chat_id, tg_user_id)
        );
        """)

        await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {T_SUM} (
            chat_id BIGINT NOT NULL,
            tg_user_id BIGINT NOT NULL,
            tg_name TEXT,
            shift_date DATE NOT NULL,
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
            PRIMARY KEY(chat_id, tg_user_id, shift_date, shift)
        );
        """)

        await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {T_ACT} (
            chat_id BIGINT NOT NULL,
            tg_user_id BIGINT NOT NULL,
            shift_date DATE NOT NULL,
            shift TEXT NOT NULL,
            kind TEXT NOT NULL,
            start_at TIMESTAMPTZ NOT NULL,
            start_msg BIGINT,
            msg1 BIGINT,
            msg2 BIGINT,
            PRIMARY KEY(chat_id, tg_user_id)
        );
        """)

        await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {T_EVT} (
            id BIGSERIAL PRIMARY KEY,
            chat_id BIGINT NOT NULL,
            tg_user_id BIGINT NOT NULL,
            tg_name TEXT,
            shift_date DATE NOT NULL,
            shift TEXT NOT NULL,
            kind TEXT NOT NULL,
            start_at TIMESTAMPTZ NOT NULL,
            end_at TIMESTAMPTZ NOT NULL,
            used_min INT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)

        await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_sum_date_shift ON {T_SUM}(chat_id, shift_date, shift);")
        await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_evt_date_shift ON {T_EVT}(chat_id, shift_date, shift);")


async def touch_user(chat_id: int, tg_user_id: int, tg_name: str):
    async with pool.acquire() as conn:
        await conn.execute(
            f"""
            INSERT INTO {T_USERS}(chat_id, tg_user_id, tg_name)
            VALUES($1,$2,$3)
            ON CONFLICT(chat_id, tg_user_id) DO UPDATE
            SET tg_name=EXCLUDED.tg_name, last_seen=NOW()
            """,
            chat_id, tg_user_id, tg_name
        )


async def ensure_sum_row(chat_id: int, tg_user_id: int, tg_name: str, shift_date: date, shift: str):
    async with pool.acquire() as conn:
        await conn.execute(
            f"""
            INSERT INTO {T_SUM}(chat_id, tg_user_id, tg_name, shift_date, shift)
            VALUES($1,$2,$3,$4,$5)
            ON CONFLICT(chat_id, tg_user_id, shift_date, shift) DO UPDATE
            SET tg_name=EXCLUDED.tg_name, updated_at=NOW()
            """,
            chat_id, tg_user_id, tg_name, shift_date, shift
        )


async def get_active(chat_id: int, tg_user_id: int):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            f"SELECT * FROM {T_ACT} WHERE chat_id=$1 AND tg_user_id=$2",
            chat_id, tg_user_id
        )


async def set_active(chat_id: int, tg_user_id: int, shift_date: date, shift: str, kind: str,
                     start_at: datetime, start_msg: int, msg1: int, msg2: int):
    async with pool.acquire() as conn:
        await conn.execute(
            f"""
            INSERT INTO {T_ACT}(chat_id, tg_user_id, shift_date, shift, kind, start_at, start_msg, msg1, msg2)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
            ON CONFLICT(chat_id, tg_user_id) DO UPDATE
            SET shift_date=EXCLUDED.shift_date,
                shift=EXCLUDED.shift,
                kind=EXCLUDED.kind,
                start_at=EXCLUDED.start_at,
                start_msg=EXCLUDED.start_msg,
                msg1=EXCLUDED.msg1,
                msg2=EXCLUDED.msg2
            """,
            chat_id, tg_user_id, shift_date, shift, kind, start_at, start_msg, msg1, msg2
        )


async def clear_active(chat_id: int, tg_user_id: int):
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT * FROM {T_ACT} WHERE chat_id=$1 AND tg_user_id=$2",
            chat_id, tg_user_id
        )
        await conn.execute(
            f"DELETE FROM {T_ACT} WHERE chat_id=$1 AND tg_user_id=$2",
            chat_id, tg_user_id
        )
        return row


async def get_kind_count(chat_id: int, tg_user_id: int, shift_date: date, shift: str, kind: str) -> int:
    col = f"{kind}_count"
    async with pool.acquire() as conn:
        v = await conn.fetchval(
            f"""
            SELECT {col} FROM {T_SUM}
            WHERE chat_id=$1 AND tg_user_id=$2 AND shift_date=$3 AND shift=$4
            """,
            chat_id, tg_user_id, shift_date, shift
        )
    return int(v or 0)


async def add_break_to_sum(chat_id: int, tg_user_id: int, shift_date: date, shift: str, kind: str, used_min: int):
    count_col = f"{kind}_count"
    min_col = f"{kind}_min"
    async with pool.acquire() as conn:
        await conn.execute(
            f"""
            UPDATE {T_SUM}
            SET {count_col} = {count_col} + 1,
                {min_col}   = {min_col} + $1,
                updated_at  = NOW()
            WHERE chat_id=$2 AND tg_user_id=$3 AND shift_date=$4 AND shift=$5
            """,
            used_min, chat_id, tg_user_id, shift_date, shift
        )


async def insert_event(chat_id: int, tg_user_id: int, tg_name: str, shift_date: date, shift: str,
                       kind: str, start_at: datetime, end_at: datetime, used_min: int):
    async with pool.acquire() as conn:
        await conn.execute(
            f"""
            INSERT INTO {T_EVT}(chat_id, tg_user_id, tg_name, shift_date, shift, kind, start_at, end_at, used_min)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
            """,
            chat_id, tg_user_id, tg_name, shift_date, shift, kind, start_at, end_at, used_min
        )


async def fetch_export_sum(chat_id: int, d: date):
    async with pool.acquire() as conn:
        return await conn.fetch(
            f"""
            SELECT shift_date, shift, tg_user_id, tg_name,
                   pee_count, pee_min, poop_count, poop_min,
                   meal_count, meal_min, smoke_count, smoke_min
            FROM {T_SUM}
            WHERE chat_id=$1 AND shift_date=$2
            ORDER BY shift ASC, tg_user_id ASC
            """,
            chat_id, d
        )


async def fetch_export_evt(chat_id: int, d: date):
    async with pool.acquire() as conn:
        return await conn.fetch(
            f"""
            SELECT shift_date, shift, tg_user_id, tg_name, kind, start_at, end_at, used_min
            FROM {T_EVT}
            WHERE chat_id=$1 AND shift_date=$2
            ORDER BY shift ASC, tg_user_id ASC, start_at ASC
            """,
            chat_id, d
        )


async def fetch_missed(chat_id: int, d: date):
    async with pool.acquire() as conn:
        seen = await conn.fetch(
            f"SELECT tg_user_id, tg_name FROM {T_USERS} WHERE chat_id=$1 ORDER BY tg_user_id ASC",
            chat_id
        )
        present_shift = await conn.fetch(
            f"""
            SELECT DISTINCT tg_user_id, shift
            FROM {T_SUM}
            WHERE chat_id=$1 AND shift_date=$2
            """,
            chat_id, d
        )

    seen_users = [(int(r["tg_user_id"]), (r["tg_name"] or "").strip()) for r in seen]
    present_day = set()
    present_day_shift = {"白班": set(), "夜班": set()}
    for r in present_shift:
        uid = int(r["tg_user_id"])
        sh = r["shift"]
        present_day.add(uid)
        if sh in present_day_shift:
            present_day_shift[sh].add(uid)

    return seen_users, present_day, present_day_shift


# =========================
# 指令
# =========================
@dp.message(CommandStart())
async def start_cmd(message: Message):
    if message.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.reply(
            "✅ 打卡机器人已启用（斯里兰卡时间 Asia/Colombo）\n\n"
            "记录：/meal 吃饭、/pee 小便、/poop 大便、/smoke 抽烟\n"
            "结束：/back 回来（结算时长）\n"
            "白班：07:00-18:59；夜班：19:00-06:59（凌晨归前一天夜班）\n\n"
            "导出（管理员）：/export 2026-02-08\n"
            "缺卡（管理员）：/missed 2026-02-08\n",
            reply_markup=KB
        )
    else:
        await message.reply("请把机器人拉进群使用。", reply_markup=KB)


@dp.message(Command("export"))
async def export_cmd(message: Message):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return await message.reply("请在群里导出。")

    if not ADMIN_IDS:
        return await message.reply("未配置 ADMIN_IDS，当前禁止导出。")
    if message.from_user.id not in ADMIN_IDS:
        return await message.reply("你没有导出权限。")

    parts = (message.text or "").split()
    if len(parts) < 2:
        return await message.reply("格式：/export 2026-02-08")

    try:
        d = datetime.strptime(parts[1], "%Y-%m-%d").date()
    except Exception:
        return await message.reply("格式：/export 2026-02-08")

    wait = await message.reply("⏳ 正在导出请稍等…")
    try:
        sums = await fetch_export_sum(message.chat.id, d)
        evts = await fetch_export_evt(message.chat.id, d)

        buf1 = io.StringIO()
        w1 = csv.writer(buf1)
        w1.writerow([
            "班次日期(斯里兰卡)", "班次", "用户ID", "用户名",
            "小便次数", "小便总分钟",
            "大便次数", "大便总分钟",
            "吃饭次数", "吃饭总分钟",
            "抽烟次数", "抽烟总分钟",
        ])
        for r in sums:
            uid_text = "\t" + str(int(r["tg_user_id"]))
            w1.writerow([
                str(r["shift_date"]), r["shift"], uid_text, (r["tg_name"] or "").strip(),
                int(r["pee_count"]), int(r["pee_min"]),
                int(r["poop_count"]), int(r["poop_min"]),
                int(r["meal_count"]), int(r["meal_min"]),
                int(r["smoke_count"]), int(r["smoke_min"]),
            ])

        buf2 = io.StringIO()
        w2 = csv.writer(buf2)
        w2.writerow([
            "班次日期(斯里兰卡)", "班次", "用户ID", "用户名",
            "类型", "开始时间(斯里兰卡)", "结束时间(斯里兰卡)", "用时(分钟)"
        ])
        for e in evts:
            uid_text = "\t" + str(int(e["tg_user_id"]))
            w2.writerow([
                str(e["shift_date"]),
                e["shift"],
                uid_text,
                (e["tg_name"] or "").strip(),
                KIND_CN.get(e["kind"], e["kind"]),
                e["start_at"].astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S"),
                e["end_at"].astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S"),
                int(e["used_min"]),
            ])

        await message.answer_document(BufferedInputFile(buf1.getvalue().encode("utf-8-sig"),
                                                      filename=f"打卡汇总_{message.chat.id}_{d}.csv"))
        await message.answer_document(BufferedInputFile(buf2.getvalue().encode("utf-8-sig"),
                                                      filename=f"打卡明细_{message.chat.id}_{d}.csv"))
        await wait.edit_text("✅ 导出完成（汇总 + 明细）")
    except Exception as e:
        await wait.edit_text(f"❌ 导出失败：{type(e).__name__}: {e}")


@dp.message(Command("missed"))
async def missed_cmd(message: Message):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return await message.reply("请在群里使用。")

    if not ADMIN_IDS:
        return await message.reply("未配置 ADMIN_IDS，当前禁止查询缺卡。")
    if message.from_user.id not in ADMIN_IDS:
        return await message.reply("你没有权限。")

    parts = (message.text or "").split()
    if len(parts) < 2:
        return await message.reply("格式：/missed 2026-02-08")

    try:
        d = datetime.strptime(parts[1], "%Y-%m-%d").date()
    except Exception:
        return await message.reply("格式：/missed 2026-02-08")

    seen_users, present_day, present_day_shift = await fetch_missed(message.chat.id, d)

    missed_all = [(uid, name) for uid, name in seen_users if uid not in present_day]
    missed_day = [(uid, name) for uid, name in seen_users if uid not in present_day_shift["白班"]]
    missed_night = [(uid, name) for uid, name in seen_users if uid not in present_day_shift["夜班"]]

    def fmt(lst):
        if not lst:
            return "（无）"
        return "\n".join([f"- {(name if name else str(uid))} ({uid})" for uid, name in lst])

    await message.reply(
        f"🧾 缺卡统计（{d}，斯里兰卡）\n\n"
        f"❌ 一天都没打卡：\n{fmt(missed_all)}\n\n"
        f"❌ 白班没打卡：\n{fmt(missed_day)}\n\n"
        f"❌ 夜班没打卡：\n{fmt(missed_night)}",
        reply_markup=KB
    )


# =========================
# 记录“出现过的账号”：任何群消息都记
# =========================
@dp.message(F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
async def on_any_group_message(message: Message):
    if message.from_user:
        await touch_user(message.chat.id, message.from_user.id, get_tg_name(message))


# =========================
# 休息入口
# =========================
@dp.message(F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}) & F.text)
async def on_group_text(message: Message):
    raw = (message.text or "").strip()
    raw_lower = raw.lower()

    m = re.search(r"^/[a-z]+", raw_lower)  # 必须从开头匹配
    cmd = m.group(0) if m else ""

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
    shift, shift_date = infer_shift_and_date(now)

    active = await get_active(chat_id, tg_user_id)
    if active and kind != "back":
        return await message.reply("⚠️ 你当前还有进行中的状态，请先点【/back 回来】再继续。", reply_markup=KB)

    if kind == "back":
        if not active:
            return await message.reply("你当前没有进行中的记录。", reply_markup=KB)

        act = await clear_active(chat_id, tg_user_id)
        used_min = int(max(0, (now - act["start_at"]).total_seconds() // 60))
        bk = act["kind"]
        act_shift = act["shift"]
        act_date = act["shift_date"]

        await ensure_sum_row(chat_id, tg_user_id, tg_name, act_date, act_shift)
        await insert_event(chat_id, tg_user_id, tg_name, act_date, act_shift, bk, act["start_at"], now, used_min)
        await add_break_to_sum(chat_id, tg_user_id, act_date, act_shift, bk, used_min)

        used_cnt = await get_kind_count(chat_id, tg_user_id, act_date, act_shift, bk)
        limit = DAILY_LIMITS.get(bk, 999)
        left = max(0, limit - used_cnt)

        # 尝试删过程消息
        for mid in [act.get("start_msg"), act.get("msg1"), act.get("msg2"), message.message_id]:
            if mid:
                await safe_delete(chat_id, int(mid))

        limit_min = DEFAULT_MINUTES.get(bk, 0)
        overtime = max(0, used_min - limit_min) if limit_min else 0
        extra = ""
        if limit_min:
            extra = f"\n⏱ 超时：{overtime} 分钟（提示 {limit_min} 分钟）" if overtime > 0 else f"\n✅ 未超时（提示 {limit_min} 分钟）"

        return await message.answer(
            f"✅ {mention} 已回来：本次【{KIND_CN.get(bk, bk)}】用时 {used_min} 分钟。"
            f"{extra}\n"
            f"归属：{act_date} {act_shift}｜已用 {used_cnt}/{limit} 次，剩余 {left} 次。",
            reply_markup=KB,
            parse_mode=ParseMode.HTML
        )

    # 开始一次休息
    await ensure_sum_row(chat_id, tg_user_id, tg_name, shift_date, shift)

    used_cnt = await get_kind_count(chat_id, tg_user_id, shift_date, shift, kind)
    limit = DAILY_LIMITS.get(kind, 999)
    if used_cnt >= limit:
        return await message.reply(
            f"⛔️ {shift_date} {shift}【{KIND_CN.get(kind, kind)}】次数已满：{used_cnt}/{limit}。",
            reply_markup=KB
        )

    minutes = DEFAULT_MINUTES.get(kind, 10)
    deadline = (now + timedelta(minutes=minutes)).astimezone(TZ).strftime("%H:%M")

    msg1 = await message.answer(
        f"📝 {mention} 已记录：{KIND_CN.get(kind, kind)}（第 {used_cnt + 1}/{limit} 次）\n归属：{shift_date} {shift}",
        reply_markup=KB,
        parse_mode=ParseMode.HTML
    )
    msg2 = await message.answer(
        f"⏰ {mention} 请在 {deadline} 前回来（提示值 {minutes} 分钟）。\n结束请点【/back 回来】",
        reply_markup=KB,
        parse_mode=ParseMode.HTML
    )

    await set_active(chat_id, tg_user_id, shift_date, shift, kind, now,
                     message.message_id, msg1.message_id, msg2.message_id)


# =========================
# Railway 稳定版：自动重连 + SIGTERM 优雅退出
# =========================
_stop_event = asyncio.Event()


def _handle_sigterm():
    print("[bot] got SIGTERM -> stopping...")
    _stop_event.set()


async def run_bot_forever():
    await db_init()

    # 防 webhook 干扰 polling
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception as e:
        print("[bot] delete_webhook error:", e)

    # 轮询自动重连
    while not _stop_event.is_set():
        try:
            print("[bot] polling started")
            await dp.start_polling(bot, allowed_updates=["message"])
        except asyncio.CancelledError:
            break
        except Exception as e:
            print("[bot] polling crashed:", repr(e))
            await asyncio.sleep(2)

    print("[bot] stopped")


async def main():
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _handle_sigterm)
        except NotImplementedError:
            # Windows / 部分环境不支持
            pass

    await run_bot_forever()


if __name__ == "__main__":
    asyncio.run(main())
