import os
import io
import re
import csv
import asyncio
from datetime import datetime, timedelta, date, time
from zoneinfo import ZoneInfo
from typing import Optional, Tuple

import asyncpg
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.enums import ChatType
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
LATE_GRACE_MIN = 10            # 迟到宽限（分钟）你要改就在这里改

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
# 按天次数限制（按“工作日 work_day”，即 07:00 切日）
DAILY_LIMITS = {
    "pee": 3,     # 小便/厕所
    "poop": 2,    # 大便
    "meal": 3,    # 吃饭
    "smoke": 5,   # 抽烟
    "checkin": 1  # 上班（每个班次最多一次）
}

# 默认允许时长（分钟）——用于提示“几点前回来”，总用时实际按回来时计算
DEFAULT_MINUTES = {
    "pee": 6,
    "poop": 15,
    "meal": 30,
    "smoke": 10,
}

KIND_CN = {
    "checkin": "上班",
    "pee": "小便/厕所",
    "poop": "大便",
    "meal": "吃饭",
    "smoke": "抽烟",
}

# 群里文本识别（你也可以只用按钮）
TEXT_ALIASES = {
    "上班": "checkin",
    "开工": "checkin",
    "in": "checkin",

    "小便": "pee",
    "厕所": "pee",
    "上厕所": "pee",
    "尿": "pee",

    "大便": "poop",
    "拉屎": "poop",

    "吃饭": "meal",
    "eat": "meal",

    "抽烟": "smoke",
    "抽": "smoke",

    "回来": "back",
    "回": "back",
    "back": "back",
    "1": "back",
    "结束": "back",
}

# 键盘（像你图里那样）
KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="上班"), KeyboardButton(text="吃饭"), KeyboardButton(text="小便")],
        [KeyboardButton(text="大便"), KeyboardButton(text="抽烟"), KeyboardButton(text="回来")],
    ],
    resize_keyboard=True
)


# =========================
# 时间口径：07:00 切日 + 白夜班
# =========================
def now_sl() -> datetime:
    return datetime.now(tz=TZ)

def work_day(dt: datetime) -> date:
    """工作日标签：07:00 ~ 次日07:00 算同一天。"""
    lt = dt.astimezone(TZ)
    if lt.time() >= DAY_CUT:
        return lt.date()
    return (lt.date() - timedelta(days=1))

def infer_shift(dt: datetime) -> str:
    """自动判班：07:00-19:00 白班；19:00-次日07:00 夜班。"""
    lt = dt.astimezone(TZ)
    t = lt.time()
    if time(7, 0) <= t < time(19, 0):
        return "白班"
    return "夜班"

def shift_start_dt(wd: date, shift: str) -> datetime:
    """给定 work_day + shift，返回该班次开始时间（斯里兰卡）。"""
    if shift == "白班":
        return datetime.combine(wd, time(7, 0), TZ)
    # 夜班开始在同一个 work_day 的 19:00
    return datetime.combine(wd, time(19, 0), TZ)

def is_on_time(checkin_at: datetime, wd: date, shift: str) -> bool:
    start = shift_start_dt(wd, shift)
    return checkin_at <= (start + timedelta(minutes=LATE_GRACE_MIN))


# =========================
# DB 初始化（汇总表 + 活动状态）
# =========================
async def db_init():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)

    async with pool.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS shift_summary (
            chat_id BIGINT NOT NULL,
            tg_user_id BIGINT NOT NULL,
            work_day DATE NOT NULL,          -- 07:00 切日标签
            shift TEXT NOT NULL,             -- 白班/夜班
            checkin_at TIMESTAMPTZ,
            on_time BOOLEAN,

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

        await conn.execute("""
        CREATE TABLE IF NOT EXISTS active_session (
            chat_id BIGINT NOT NULL,
            tg_user_id BIGINT NOT NULL,
            work_day DATE NOT NULL,
            shift TEXT NOT NULL,
            kind TEXT NOT NULL,              -- pee/poop/meal/smoke
            start_at TIMESTAMPTZ NOT NULL,
            msg1 BIGINT,
            msg2 BIGINT,
            PRIMARY KEY (chat_id, tg_user_id)
        );
        """)

        await conn.execute("""
        CREATE TABLE IF NOT EXISTS user_shift_pref (
            chat_id BIGINT NOT NULL,
            tg_user_id BIGINT NOT NULL,
            shift TEXT NOT NULL,             -- 白班/夜班
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (chat_id, tg_user_id)
        );
        """)

        await conn.execute("CREATE INDEX IF NOT EXISTS idx_sum_day_shift ON shift_summary(chat_id, work_day, shift);")


# =========================
# DB 工具
# =========================
async def get_user_shift(chat_id: int, tg_user_id: int, dt: datetime) -> str:
    """优先用 /use 设置；没设置就自动判班。"""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT shift FROM user_shift_pref WHERE chat_id=$1 AND tg_user_id=$2",
            chat_id, tg_user_id
        )
    if row and row["shift"] in ("白班", "夜班"):
        return row["shift"]
    return infer_shift(dt)

async def set_user_shift(chat_id: int, tg_user_id: int, shift: str):
    async with pool.acquire() as conn:
        await conn.execute("""
        INSERT INTO user_shift_pref(chat_id, tg_user_id, shift)
        VALUES($1,$2,$3)
        ON CONFLICT(chat_id, tg_user_id) DO UPDATE
        SET shift=EXCLUDED.shift, updated_at=NOW()
        """, chat_id, tg_user_id, shift)

async def get_active(chat_id: int, tg_user_id: int):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM active_session WHERE chat_id=$1 AND tg_user_id=$2",
            chat_id, tg_user_id
        )

async def set_active(chat_id: int, tg_user_id: int, wd: date, shift: str, kind: str, start_at: datetime, msg1: int, msg2: int):
    async with pool.acquire() as conn:
        await conn.execute("""
        INSERT INTO active_session(chat_id, tg_user_id, work_day, shift, kind, start_at, msg1, msg2)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8)
        ON CONFLICT(chat_id, tg_user_id) DO UPDATE
        SET work_day=EXCLUDED.work_day,
            shift=EXCLUDED.shift,
            kind=EXCLUDED.kind,
            start_at=EXCLUDED.start_at,
            msg1=EXCLUDED.msg1,
            msg2=EXCLUDED.msg2
        """, chat_id, tg_user_id, wd, shift, kind, start_at, msg1, msg2)

async def clear_active(chat_id: int, tg_user_id: int):
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM active_session WHERE chat_id=$1 AND tg_user_id=$2", chat_id, tg_user_id)
        await conn.execute("DELETE FROM active_session WHERE chat_id=$1 AND tg_user_id=$2", chat_id, tg_user_id)
        return row

async def ensure_summary_row(chat_id: int, tg_user_id: int, wd: date, shift: str):
    async with pool.acquire() as conn:
        await conn.execute("""
        INSERT INTO shift_summary(chat_id, tg_user_id, work_day, shift)
        VALUES($1,$2,$3,$4)
        ON CONFLICT DO NOTHING
        """, chat_id, tg_user_id, wd, shift)

async def get_checkin_time(chat_id: int, tg_user_id: int, wd: date, shift: str) -> Optional[datetime]:
    async with pool.acquire() as conn:
        return await conn.fetchval("""
        SELECT checkin_at FROM shift_summary
        WHERE chat_id=$1 AND tg_user_id=$2 AND work_day=$3 AND shift=$4
        """, chat_id, tg_user_id, wd, shift)

async def get_kind_count(chat_id: int, tg_user_id: int, wd: date, shift: str, kind: str) -> int:
    col = f"{kind}_count"
    async with pool.acquire() as conn:
        return int(await conn.fetchval(f"""
        SELECT {col} FROM shift_summary
        WHERE chat_id=$1 AND tg_user_id=$2 AND work_day=$3 AND shift=$4
        """, chat_id, tg_user_id, wd, shift) or 0)

async def add_checkin(chat_id: int, tg_user_id: int, wd: date, shift: str, checkin_at: datetime):
    ot = is_on_time(checkin_at, wd, shift)
    async with pool.acquire() as conn:
        await conn.execute("""
        UPDATE shift_summary
        SET checkin_at=$1, on_time=$2, updated_at=NOW()
        WHERE chat_id=$3 AND tg_user_id=$4 AND work_day=$5 AND shift=$6
        """, checkin_at, ot, chat_id, tg_user_id, wd, shift)

async def add_break_result(chat_id: int, tg_user_id: int, wd: date, shift: str, kind: str, used_min: int):
    # 累加次数+分钟
    count_col = f"{kind}_count"
    min_col = f"{kind}_min"
    async with pool.acquire() as conn:
        await conn.execute(f"""
        UPDATE shift_summary
        SET {count_col} = {count_col} + 1,
            {min_col}   = {min_col} + $1,
            updated_at = NOW()
        WHERE chat_id=$2 AND tg_user_id=$3 AND work_day=$4 AND shift=$5
        """, used_min, chat_id, tg_user_id, wd, shift)

async def fetch_export(chat_id: int, start_day: date, end_day: date):
    async with pool.acquire() as conn:
        return await conn.fetch("""
        SELECT work_day, shift, tg_user_id, checkin_at, on_time,
               pee_count, pee_min, poop_count, poop_min,
               meal_count, meal_min, smoke_count, smoke_min
        FROM shift_summary
        WHERE chat_id=$1 AND work_day BETWEEN $2 AND $3
        ORDER BY work_day ASC, shift ASC, tg_user_id ASC
        """, chat_id, start_day, end_day)


# =========================
# 指令：/start /use /who /export
# =========================
@dp.message(CommandStart())
async def start_cmd(message: Message):
    if message.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.reply(
            "✅ 打卡机器人已启用\n\n"
            "按钮：上班 / 吃饭 / 小便 / 大便 / 抽烟 / 回来\n"
            "规则：未上班不能休息；进行中必须先回来；次数超限会拒绝。\n"
            "日统计口径：斯里兰卡时间 07:00~次日07:00 算一天。\n\n"
            "共用号：可用 /use 白班 或 /use 夜班（不设置也会自动按时间判班）\n"
            "导出：/export 2026-02-05  或 /export 2026-02-01 2026-02-05",
            reply_markup=KB
        )
    else:
        await message.reply("请把机器人拉进群使用。", reply_markup=KB)

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

    if ADMIN_IDS and (message.from_user.id not in ADMIN_IDS):
        return await message.reply("你没有导出权限。")

    parts = (message.text or "").split()
    # 默认导出“当前工作日”
    start_day = end_day = work_day(now_sl())

    def parse_d(s: str) -> Optional[date]:
        try:
            return datetime.strptime(s, "%Y-%m-%d").date()
        except Exception:
            return None

    if len(parts) == 2:
        d = parse_d(parts[1])
        if not d:
            return await message.reply("格式：/export 2026-02-05")
        start_day = end_day = d
    elif len(parts) >= 3:
        d1 = parse_d(parts[1]); d2 = parse_d(parts[2])
        if not d1 or not d2:
            return await message.reply("格式：/export 2026-02-01 2026-02-05")
        start_day, end_day = (d1, d2) if d1 <= d2 else (d2, d1)

    wait = await message.reply("⏳ 正在导出请稍等…")
    try:
        rows = await fetch_export(message.chat.id, start_day, end_day)

        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow([
            "work_day(07:00~next07:00)", "shift", "tg_user_id",
            "checkin_time", "on_time",
            "pee_count", "pee_total_min",
            "poop_count", "poop_total_min",
            "meal_count", "meal_total_min",
            "smoke_count", "smoke_total_min",
        ])

        for r in rows:
            ct = r["checkin_at"]
            w.writerow([
                str(r["work_day"]),
                r["shift"],
                int(r["tg_user_id"]),
                ct.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S") if ct else "",
                "YES" if r["on_time"] else ("NO" if ct else "NO_CHECKIN"),
                int(r["pee_count"]), int(r["pee_min"]),
                int(r["poop_count"]), int(r["poop_min"]),
                int(r["meal_count"]), int(r["meal_min"]),
                int(r["smoke_count"]), int(r["smoke_min"]),
            ])

        data = buf.getvalue().encode("utf-8-sig")
        filename = f"summary_{message.chat.id}_{start_day}_{end_day}.csv"
        doc = BufferedInputFile(data, filename=filename)
        await message.answer_document(doc)
        await wait.edit_text("✅ 导出完成")
    except Exception as e:
        await wait.edit_text(f"❌ 导出失败：{type(e).__name__}: {e}")


# =========================
# 群消息：打卡入口
# =========================
def normalize_text(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", "", s)
    return s

@dp.message(F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}) & F.text)
async def on_group_text(message: Message):
    raw = (message.text or "").strip()
    key = normalize_text(raw)

    # 允许按钮直接用中文，不强制小写
    kind = TEXT_ALIASES.get(raw) or TEXT_ALIASES.get(key)  # 兼容 back/in
    if not kind:
        return

    chat_id = message.chat.id
    tg_user_id = message.from_user.id

    now = now_sl()
    wd = work_day(now)
    shift = await get_user_shift(chat_id, tg_user_id, now)

    # 确保汇总行存在
    await ensure_summary_row(chat_id, tg_user_id, wd, shift)

    # 1) 如果有进行中，非 back 一律拦住
    active = await get_active(chat_id, tg_user_id)
    if active and kind != "back":
        return await message.reply("⚠️ 你当前还有进行中的状态，请先点【回来】再继续。", reply_markup=KB)

    # 2) back：结算一次，累加总分钟 + 次数
    if kind == "back":
        if not active:
            return await message.reply("你当前没有进行中的记录。", reply_markup=KB)

        # 取并清
        act = await clear_active(chat_id, tg_user_id)
        start_at = act["start_at"].astimezone(TZ)
        used_min = int(max(0, (now - act["start_at"]).total_seconds() // 60))
        bk = act["kind"]
        act_wd = act["work_day"]
        act_shift = act["shift"]

        # 删除两条提示
        for mid in [act["msg1"], act["msg2"]]:
            if mid:
                try:
                    await bot.delete_message(chat_id, int(mid))
                except Exception:
                    pass

        # 累加汇总
        await ensure_summary_row(chat_id, tg_user_id, act_wd, act_shift)
        await add_break_result(chat_id, tg_user_id, act_wd, act_shift, bk, used_min)

        # 剩余次数提示
        used_cnt = await get_kind_count(chat_id, tg_user_id, act_wd, act_shift, bk)
        limit = DAILY_LIMITS.get(bk, 999)
        left = max(0, limit - used_cnt)

        return await message.reply(
            f"✅ 已回来：本次【{KIND_CN.get(bk,bk)}】用时 {used_min} 分钟。\n"
            f"今日（{act_wd} {act_shift}）已用 {used_cnt}/{limit} 次，剩余 {left} 次。",
            reply_markup=KB
        )

    # 3) checkin：上班（允许在任何时间打，但会判定是否按时；同一班次只允许一次）
    if kind == "checkin":
        exist = await get_checkin_time(chat_id, tg_user_id, wd, shift)
        if exist:
            return await message.reply("⛔️ 本班次已经打过上班了。", reply_markup=KB)

        await add_checkin(chat_id, tg_user_id, wd, shift, now)
        ot = is_on_time(now, wd, shift)
        return await message.reply(
            f"✅ 上班打卡成功（{wd} {shift}）。\n"
            f"{'✅ 按时' if ot else '⚠️ 已迟到'}（宽限 {LATE_GRACE_MIN} 分钟）",
            reply_markup=KB
        )

    # 4) 其它类型：没上班不能开始
    checkin_at = await get_checkin_time(chat_id, tg_user_id, wd, shift)
    if not checkin_at:
        return await message.reply(
            f"⛔️ 你还没打上班（{wd} {shift}），不能进行【{KIND_CN.get(kind,kind)}】。\n"
            f"请先点【上班】。",
            reply_markup=KB
        )

    # 5) 次数限制
    used_cnt = await get_kind_count(chat_id, tg_user_id, wd, shift, kind)
    limit = DAILY_LIMITS.get(kind, 999)
    if used_cnt >= limit:
        return await message.reply(
            f"⛔️ 今日（{wd} {shift}）【{KIND_CN.get(kind,kind)}】次数已满：{used_cnt}/{limit}。",
            reply_markup=KB
        )

    # 6) 开始一次：记录 active，并提示“几点前回来”
    minutes = DEFAULT_MINUTES.get(kind, 10)
    deadline = (now + timedelta(minutes=minutes)).astimezone(TZ).strftime("%H:%M")

    msg1 = await message.reply(
        f"📝 已记录：{KIND_CN.get(kind,kind)}（第 {used_cnt+1}/{limit} 次）",
        reply_markup=KB
    )
    msg2 = await message.reply(
        f"⏰ 请在 {deadline} 前回来（提示值 {minutes} 分钟）。\n"
        f"回来请点【回来】或发：回 / back / 1 / 结束",
        reply_markup=KB
    )

    await set_active(chat_id, tg_user_id, wd, shift, kind, now, msg1.message_id, msg2.message_id)


# =========================
# 启动
# =========================
async def main():
    await db_init()
    # 避免 webhook 残留导致“没反应”
    await bot.delete_webhook(drop_pending_updates=True)
    print("[bot] polling started")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
