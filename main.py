import os
import io
import re
import csv
import asyncio
import html
import random
from datetime import datetime, timedelta, date
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

# ✅ 越南时间
TZ = ZoneInfo("Asia/Ho_Chi_Minh")


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
# 业务规则
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
    "🌟 加油！你今天一定很顺。",
    "🧠 先把最重要的事搞定，后面就轻松了。",
]

CHECKOUT_QUOTES = [
    "👏 辛苦了！收工休息一下。",
    "✅ 下班啦，今天表现不错。",
    "🌙 结束一班，早点放松。",
    "💯 做得好，明天继续保持。",
]

# ✅ 命令映射（隐私模式也能收到）
CMD_ALIASES = {
    "/in": "checkin",
    "/out": "checkout",
    "/meal": "meal",
    "/pee": "pee",
    "/poop": "poop",
    "/smoke": "smoke",
    "/back": "back",
    "/export": "export",
}

# 可选：纯文字（隐私模式开着可能收不到，但不影响 /命令）
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
}

# ✅ 中文键盘（仍然是 /命令）
KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="✅ /in 上班"), KeyboardButton(text="🏁 /out 下班")],
        [KeyboardButton(text="🍚 /meal 吃饭"), KeyboardButton(text="🚽 /pee 小便"), KeyboardButton(text="💩 /poop 大便")],
        [KeyboardButton(text="🚬 /smoke 抽烟"), KeyboardButton(text="↩️ /back 回来")],
        [KeyboardButton(text="📤 /export 导出")],
    ],
    resize_keyboard=True
)


# =========================
# 时间口径：按“上班当天”归档
# work_day = 上班那天（越南日期）
# =========================
def now_vn() -> datetime:
    return datetime.now(tz=TZ)


def vn_date(dt: datetime) -> date:
    return dt.astimezone(TZ).date()


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
# DB（新表名，避免旧库冲突）
# =========================
TABLE_SHIFT = "shift_record_vn_v4"       # 一次上班到下班（一条）
TABLE_ACT = "active_session_vn_v4"      # 当前休息进行中（每人最多一条）
TABLE_EVT = "break_event_vn_v4"         # 每次吃饭/小便/大便/抽烟的明细


async def db_init():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)

    async with pool.acquire() as conn:
        # 一次上班记录
        await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_SHIFT} (
            id BIGSERIAL PRIMARY KEY,
            chat_id BIGINT NOT NULL,
            tg_user_id BIGINT NOT NULL,
            tg_name TEXT,
            work_day DATE NOT NULL,              -- ✅ 上班当天（越南日期）
            checkin_at TIMESTAMPTZ NOT NULL,
            checkout_at TIMESTAMPTZ,

            pee_count INT NOT NULL DEFAULT 0,
            pee_min   INT NOT NULL DEFAULT 0,
            poop_count INT NOT NULL DEFAULT 0,
            poop_min   INT NOT NULL DEFAULT 0,
            meal_count INT NOT NULL DEFAULT 0,
            meal_min   INT NOT NULL DEFAULT 0,
            smoke_count INT NOT NULL DEFAULT 0,
            smoke_min   INT NOT NULL DEFAULT 0,

            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)

        await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_shift_open_v4 ON {TABLE_SHIFT}(chat_id, tg_user_id) WHERE checkout_at IS NULL;")
        await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_shift_day_v4 ON {TABLE_SHIFT}(chat_id, work_day);")
        await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_shift_user_day_v4 ON {TABLE_SHIFT}(chat_id, tg_user_id, work_day);")

        # 休息进行中
        await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_ACT} (
            chat_id BIGINT NOT NULL,
            tg_user_id BIGINT NOT NULL,
            shift_id BIGINT NOT NULL,
            kind TEXT NOT NULL,                   -- pee/poop/meal/smoke
            start_at TIMESTAMPTZ NOT NULL,
            start_msg BIGINT,
            msg1 BIGINT,
            msg2 BIGINT,
            PRIMARY KEY (chat_id, tg_user_id)
        );
        """)

        # 休息明细
        await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_EVT} (
            id BIGSERIAL PRIMARY KEY,
            chat_id BIGINT NOT NULL,
            shift_id BIGINT NOT NULL,
            tg_user_id BIGINT NOT NULL,
            tg_name TEXT,
            kind TEXT NOT NULL,                   -- pee/poop/meal/smoke
            start_at TIMESTAMPTZ NOT NULL,
            end_at   TIMESTAMPTZ NOT NULL,
            used_min INT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)
        await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_evt_shift_v4 ON {TABLE_EVT}(chat_id, shift_id);")
        await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_evt_user_v4 ON {TABLE_EVT}(chat_id, tg_user_id);")


# =========================
# DB 工具
# =========================
async def get_open_shift(chat_id: int, tg_user_id: int):
    """当前未下班的那条上班记录（允许跨天）"""
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            f"""
            SELECT * FROM {TABLE_SHIFT}
            WHERE chat_id=$1 AND tg_user_id=$2 AND checkout_at IS NULL
            ORDER BY checkin_at DESC
            LIMIT 1
            """,
            chat_id, tg_user_id
        )


async def create_shift(chat_id: int, tg_user_id: int, tg_name: str, checkin_at: datetime) -> int:
    wd = vn_date(checkin_at)  # ✅ work_day = 上班当天
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            INSERT INTO {TABLE_SHIFT}(chat_id, tg_user_id, tg_name, work_day, checkin_at)
            VALUES($1,$2,$3,$4,$5)
            RETURNING id
            """,
            chat_id, tg_user_id, tg_name, wd, checkin_at
        )
        return int(row["id"])


async def set_shift_checkout(chat_id: int, tg_user_id: int, checkout_at: datetime):
    async with pool.acquire() as conn:
        await conn.execute(
            f"""
            UPDATE {TABLE_SHIFT}
            SET checkout_at=$1, updated_at=NOW()
            WHERE id = (
                SELECT id FROM {TABLE_SHIFT}
                WHERE chat_id=$2 AND tg_user_id=$3 AND checkout_at IS NULL
                ORDER BY checkin_at DESC
                LIMIT 1
            )
            """,
            checkout_at, chat_id, tg_user_id
        )


async def get_active(chat_id: int, tg_user_id: int):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            f"SELECT * FROM {TABLE_ACT} WHERE chat_id=$1 AND tg_user_id=$2",
            chat_id, tg_user_id
        )


async def set_active(chat_id: int, tg_user_id: int, shift_id: int, kind: str,
                     start_at: datetime, start_msg: int, msg1: int, msg2: int):
    async with pool.acquire() as conn:
        await conn.execute(
            f"""
            INSERT INTO {TABLE_ACT}(chat_id, tg_user_id, shift_id, kind, start_at, start_msg, msg1, msg2)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT(chat_id, tg_user_id) DO UPDATE
            SET shift_id=EXCLUDED.shift_id,
                kind=EXCLUDED.kind,
                start_at=EXCLUDED.start_at,
                start_msg=EXCLUDED.start_msg,
                msg1=EXCLUDED.msg1,
                msg2=EXCLUDED.msg2
            """,
            chat_id, tg_user_id, shift_id, kind, start_at, start_msg, msg1, msg2
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


async def get_kind_count_by_shift(shift_id: int, kind: str) -> int:
    col = f"{kind}_count"
    async with pool.acquire() as conn:
        v = await conn.fetchval(
            f"SELECT {col} FROM {TABLE_SHIFT} WHERE id=$1",
            shift_id
        )
    return int(v or 0)


async def add_break_to_shift(shift_id: int, kind: str, used_min: int):
    count_col = f"{kind}_count"
    min_col = f"{kind}_min"
    async with pool.acquire() as conn:
        await conn.execute(
            f"""
            UPDATE {TABLE_SHIFT}
            SET {count_col} = {count_col} + 1,
                {min_col}   = {min_col} + $2,
                updated_at  = NOW()
            WHERE id=$1
            """,
            shift_id, used_min
        )


async def insert_break_event(chat_id: int, shift_id: int, tg_user_id: int, tg_name: str,
                            kind: str, start_at: datetime, end_at: datetime, used_min: int):
    async with pool.acquire() as conn:
        await conn.execute(
            f"""
            INSERT INTO {TABLE_EVT}(chat_id, shift_id, tg_user_id, tg_name, kind, start_at, end_at, used_min)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8)
            """,
            chat_id, shift_id, tg_user_id, tg_name, kind, start_at, end_at, used_min
        )


async def fetch_export_shifts(chat_id: int, start_day: date, end_day: date):
    async with pool.acquire() as conn:
        return await conn.fetch(
            f"""
            SELECT id, work_day, tg_user_id, tg_name, checkin_at, checkout_at,
                   pee_count, pee_min, poop_count, poop_min,
                   meal_count, meal_min, smoke_count, smoke_min
            FROM {TABLE_SHIFT}
            WHERE chat_id=$1 AND work_day BETWEEN $2 AND $3
            ORDER BY work_day ASC, tg_user_id ASC, checkin_at ASC
            """,
            chat_id, start_day, end_day
        )


async def fetch_export_events(chat_id: int, start_day: date, end_day: date):
    async with pool.acquire() as conn:
        # 通过 shift 的 work_day 过滤明细（保证“按上班当天归档”）
        return await conn.fetch(
            f"""
            SELECT s.work_day, e.tg_user_id, e.tg_name, e.kind, e.start_at, e.end_at, e.used_min
            FROM {TABLE_EVT} e
            JOIN {TABLE_SHIFT} s ON s.id = e.shift_id
            WHERE e.chat_id=$1 AND s.work_day BETWEEN $2 AND $3
            ORDER BY s.work_day ASC, e.tg_user_id ASC, e.start_at ASC
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
            "✅ 打卡机器人已启用（越南时间 UTC+7）\n\n"
            "归档口径：按【上班当天】归档。\n"
            "例如：6号上班、7号下班 -> 记在 6号；中间休息明细也记在 6号。\n\n"
            "按钮/命令：\n"
            "/in 上班  |  /out 下班\n"
            "/meal 吃饭 | /pee 小便 | /poop 大便 | /smoke 抽烟\n"
            "/back 回来（结束本次休息并结算）\n\n"
            "导出（仅管理员）：/export 2026-02-06  或  /export 2026-02-01 2026-02-06\n",
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
    today = vn_date(now_vn())
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
        shifts = await fetch_export_shifts(message.chat.id, start_day, end_day)
        events = await fetch_export_events(message.chat.id, start_day, end_day)

        # ===== 汇总 CSV =====
        buf1 = io.StringIO()
        w1 = csv.writer(buf1)
        w1.writerow([
            "日期(按上班当天,越南)",
            "用户ID",
            "用户名",
            "上班时间(越南)",
            "下班时间(越南)",
            "小便次数", "小便总分钟",
            "大便次数", "大便总分钟",
            "吃饭次数", "吃饭总分钟",
            "抽烟次数", "抽烟总分钟",
        ])

        for r in shifts:
            uid_text = "\t" + str(int(r["tg_user_id"]))  # 防止Excel科学计数法
            name_text = (r["tg_name"] or "").strip()
            ci = r["checkin_at"]
            co = r["checkout_at"]
            ci_text = ci.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S") if ci else ""
            co_text = co.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S") if co else ""

            w1.writerow([
                str(r["work_day"]),
                uid_text,
                name_text,
                ci_text,
                co_text,
                int(r["pee_count"]), int(r["pee_min"]),
                int(r["poop_count"]), int(r["poop_min"]),
                int(r["meal_count"]), int(r["meal_min"]),
                int(r["smoke_count"]), int(r["smoke_min"]),
            ])

        data1 = buf1.getvalue().encode("utf-8-sig")
        file1 = BufferedInputFile(data1, filename=f"打卡汇总_{message.chat.id}_{start_day}_{end_day}.csv")

        # ===== 明细 CSV =====
        buf2 = io.StringIO()
        w2 = csv.writer(buf2)
        w2.writerow([
            "日期(按上班当天,越南)",
            "用户ID",
            "用户名",
            "类型",
            "开始时间(越南)",
            "结束时间(越南)",
            "用时(分钟)",
        ])

        for e in events:
            uid_text = "\t" + str(int(e["tg_user_id"]))
            name_text = (e["tg_name"] or "").strip()
            kind = e["kind"]
            kind_cn = KIND_CN.get(kind, kind)
            s = e["start_at"].astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S")
            t = e["end_at"].astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S")
            w2.writerow([
                str(e["work_day"]),
                uid_text,
                name_text,
                kind_cn,
                s,
                t,
                int(e["used_min"]),
            ])

        data2 = buf2.getvalue().encode("utf-8-sig")
        file2 = BufferedInputFile(data2, filename=f"打卡明细_{message.chat.id}_{start_day}_{end_day}.csv")

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

    # 抓第一个 /xxx（适配“✅ /in 上班”）
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
    now = now_vn()

    # 导出按钮等同 /export
    if kind == "export":
        message.text = "/export"
        return await export_cmd(message)

    # 进行中的休息：除 back 外都拦住
    active = await get_active(chat_id, tg_user_id)
    if active and kind != "back":
        return await message.reply("⚠️ 你当前还有进行中的状态，请先点【/back 回来】再继续。", reply_markup=KB)

    # back：结束休息并记录明细 + 汇总累加
    if kind == "back":
        if not active:
            return await message.reply("你当前没有进行中的记录。", reply_markup=KB)

        act = await clear_active(chat_id, tg_user_id)
        used_min = int(max(0, (now - act["start_at"]).total_seconds() // 60))
        bk = act["kind"]
        shift_id = int(act["shift_id"])

        # 写明细
        await insert_break_event(
            chat_id=chat_id,
            shift_id=shift_id,
            tg_user_id=tg_user_id,
            tg_name=tg_name,
            kind=bk,
            start_at=act["start_at"],
            end_at=now,
            used_min=used_min
        )
        # 汇总累加
        await add_break_to_shift(shift_id, bk, used_min)

        used_cnt = await get_kind_count_by_shift(shift_id, bk)
        limit = DAILY_LIMITS.get(bk, 999)
        left = max(0, limit - used_cnt)

        # 删除过程消息（可删就删）
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

        return await message.answer(
            f"✅ {mention} 已回来：本次【{KIND_CN.get(bk, bk)}】用时 {used_min} 分钟。"
            f"{extra}\n"
            f"本次上班档（按上班当天归档）已用 {used_cnt}/{limit} 次，剩余 {left} 次。",
            reply_markup=KB,
            parse_mode=ParseMode.HTML
        )

    # /in：上班（work_day = 上班当天）
    if kind == "checkin":
        open_row = await get_open_shift(chat_id, tg_user_id)
        if open_row:
            return await message.reply("⛔️ 你当前还有未下班的记录，不能重复上班。请先 /out 下班。", reply_markup=KB)

        shift_id = await create_shift(chat_id, tg_user_id, tg_name, now)
        quote = random.choice(CHECKIN_QUOTES)
        return await message.answer(
            f"✅ {mention} 上班打卡成功（越南时间）：{now.astimezone(TZ).strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"📌 归档日期（按上班当天）：{vn_date(now)}\n"
            f"{quote}",
            reply_markup=KB,
            parse_mode=ParseMode.HTML
        )

    # /out：下班（允许跨天）
    if kind == "checkout":
        open_row = await get_open_shift(chat_id, tg_user_id)
        if not open_row:
            return await message.reply("⛔️ 你当前没有未下班的上班记录。请先 /in 上班。", reply_markup=KB)

        start_at = open_row["checkin_at"]
        work_day = open_row["work_day"]  # ✅ 仍归档到上班当天
        await set_shift_checkout(chat_id, tg_user_id, now)

        used_min = int(max(0, (now - start_at).total_seconds() // 60))
        quote = random.choice(CHECKOUT_QUOTES)

        return await message.answer(
            f"✅ {mention} 下班打卡成功（越南时间）：{now.astimezone(TZ).strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"📌 归档日期（按上班当天）：{work_day}\n"
            f"⏱ 本次在岗时长（上班→下班）：{used_min} 分钟（{used_min/60:.2f} 小时）\n"
            f"{quote}",
            reply_markup=KB,
            parse_mode=ParseMode.HTML
        )

    # 休息：必须先上班且未下班
    open_row = await get_open_shift(chat_id, tg_user_id)
    if not open_row:
        return await message.reply("⛔️ 你还没上班或已经下班，不能开始休息。请先 /in 上班。", reply_markup=KB)

    # 次数限制：按这次上班记录的 shift_id 统计（不会跨天刷新）
    shift_id = int(open_row["id"])
    used_cnt = await get_kind_count_by_shift(shift_id, kind)
    limit = DAILY_LIMITS.get(kind, 999)
    if used_cnt >= limit:
        return await message.reply(
            f"⛔️ 本次上班档【{KIND_CN.get(kind, kind)}】次数已满：{used_cnt}/{limit}。",
            reply_markup=KB
        )

    minutes = DEFAULT_MINUTES.get(kind, 10)
    deadline = (now + timedelta(minutes=minutes)).astimezone(TZ).strftime("%H:%M")

    msg1 = await message.answer(
        f"📝 {mention} 已记录：{KIND_CN.get(kind, kind)}（第 {used_cnt + 1}/{limit} 次）\n"
        f"📌 归档日期：{open_row['work_day']}（按上班当天）",
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
        chat_id, tg_user_id, shift_id, kind, now,
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
