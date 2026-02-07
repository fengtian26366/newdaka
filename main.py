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

# ✅ 越南时间 UTC+7
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
# 规则（按自然日）
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

TEXT_ALIASES = {
    # 上下班
    "上班": "checkin",
    "开工": "checkin",
    "in": "checkin",

    "下班": "checkout",
    "收工": "checkout",
    "out": "checkout",

    # 休息
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

    # 回来结算
    "回来": "back",
    "回": "back",
    "back": "back",
    "1": "back",
    "结束": "back",
}

KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="上班"), KeyboardButton(text="下班")],
        [KeyboardButton(text="吃饭"), KeyboardButton(text="小便"), KeyboardButton(text="大便")],
        [KeyboardButton(text="抽烟"), KeyboardButton(text="回来")],
    ],
    resize_keyboard=True
)


# =========================
# 时间口径：越南自然日（00:00~23:59）
# =========================
def now_vn() -> datetime:
    return datetime.now(tz=TZ)


def work_day(dt: datetime) -> date:
    # ✅ 自然日，不切 07:00
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


def normalize_text(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", "", s)
    return s


# =========================
# DB 初始化（汇总表 + 活动状态）
# =========================
async def db_init():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)

    async with pool.acquire() as conn:
        # ✅ 每人每天一行（自然日）
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS shift_summary (
            chat_id BIGINT NOT NULL,
            tg_user_id BIGINT NOT NULL,
            tg_name TEXT,
            work_day DATE NOT NULL,          -- 越南自然日
            checkin_at TIMESTAMPTZ,
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
            PRIMARY KEY (chat_id, tg_user_id, work_day)
        );
        """)

        # ✅ 进行中的休息状态（一个人同一时间只能一个）
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS active_session (
            chat_id BIGINT NOT NULL,
            tg_user_id BIGINT NOT NULL,
            work_day DATE NOT NULL,
            kind TEXT NOT NULL,              -- pee/poop/meal/smoke
            start_at TIMESTAMPTZ NOT NULL,
            start_msg BIGINT,                -- 用户开始那条消息ID
            msg1 BIGINT,                     -- 机器人提示1
            msg2 BIGINT,                     -- 机器人提示2
            PRIMARY KEY (chat_id, tg_user_id)
        );
        """)

        await conn.execute("CREATE INDEX IF NOT EXISTS idx_sum_day ON shift_summary(chat_id, work_day);")


# =========================
# DB 工具
# =========================
async def ensure_summary_row(chat_id: int, tg_user_id: int, tg_name: str, wd: date):
    async with pool.acquire() as conn:
        await conn.execute("""
        INSERT INTO shift_summary(chat_id, tg_user_id, tg_name, work_day)
        VALUES($1,$2,$3,$4)
        ON CONFLICT(chat_id, tg_user_id, work_day)
        DO UPDATE SET tg_name=EXCLUDED.tg_name, updated_at=NOW()
        """, chat_id, tg_user_id, tg_name, wd)


async def get_checkin_time(chat_id: int, tg_user_id: int, wd: date) -> Optional[datetime]:
    async with pool.acquire() as conn:
        return await conn.fetchval("""
        SELECT checkin_at FROM shift_summary
        WHERE chat_id=$1 AND tg_user_id=$2 AND work_day=$3
        """, chat_id, tg_user_id, wd)


async def get_checkout_time(chat_id: int, tg_user_id: int, wd: date) -> Optional[datetime]:
    async with pool.acquire() as conn:
        return await conn.fetchval("""
        SELECT checkout_at FROM shift_summary
        WHERE chat_id=$1 AND tg_user_id=$2 AND work_day=$3
        """, chat_id, tg_user_id, wd)


async def set_checkin(chat_id: int, tg_user_id: int, wd: date, checkin_at: datetime):
    async with pool.acquire() as conn:
        await conn.execute("""
        UPDATE shift_summary
        SET checkin_at=$1, updated_at=NOW()
        WHERE chat_id=$2 AND tg_user_id=$3 AND work_day=$4
        """, checkin_at, chat_id, tg_user_id, wd)


async def set_checkout(chat_id: int, tg_user_id: int, wd: date, checkout_at: datetime):
    async with pool.acquire() as conn:
        await conn.execute("""
        UPDATE shift_summary
        SET checkout_at=$1, updated_at=NOW()
        WHERE chat_id=$2 AND tg_user_id=$3 AND work_day=$4
        """, checkout_at, chat_id, tg_user_id, wd)


async def get_kind_count(chat_id: int, tg_user_id: int, wd: date, kind: str) -> int:
    col = f"{kind}_count"
    async with pool.acquire() as conn:
        return int(await conn.fetchval(f"""
        SELECT {col} FROM shift_summary
        WHERE chat_id=$1 AND tg_user_id=$2 AND work_day=$3
        """, chat_id, tg_user_id, wd) or 0)


async def add_break_result(chat_id: int, tg_user_id: int, wd: date, kind: str, used_min: int):
    count_col = f"{kind}_count"
    min_col = f"{kind}_min"
    async with pool.acquire() as conn:
        await conn.execute(f"""
        UPDATE shift_summary
        SET {count_col} = {count_col} + 1,
            {min_col}   = {min_col} + $1,
            updated_at = NOW()
        WHERE chat_id=$2 AND tg_user_id=$3 AND work_day=$4
        """, used_min, chat_id, tg_user_id, wd)


async def get_active(chat_id: int, tg_user_id: int):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM active_session WHERE chat_id=$1 AND tg_user_id=$2",
            chat_id, tg_user_id
        )


async def set_active(chat_id: int, tg_user_id: int, wd: date, kind: str,
                     start_at: datetime, start_msg: int, msg1: int, msg2: int):
    async with pool.acquire() as conn:
        await conn.execute("""
        INSERT INTO active_session(chat_id, tg_user_id, work_day, kind, start_at, start_msg, msg1, msg2)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8)
        ON CONFLICT(chat_id, tg_user_id) DO UPDATE
        SET work_day=EXCLUDED.work_day,
            kind=EXCLUDED.kind,
            start_at=EXCLUDED.start_at,
            start_msg=EXCLUDED.start_msg,
            msg1=EXCLUDED.msg1,
            msg2=EXCLUDED.msg2
        """, chat_id, tg_user_id, wd, kind, start_at, start_msg, msg1, msg2)


async def clear_active(chat_id: int, tg_user_id: int):
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM active_session WHERE chat_id=$1 AND tg_user_id=$2",
            chat_id, tg_user_id
        )
        await conn.execute("DELETE FROM active_session WHERE chat_id=$1 AND tg_user_id=$2", chat_id, tg_user_id)
        return row


async def fetch_export(chat_id: int, start_day: date, end_day: date):
    async with pool.acquire() as conn:
        return await conn.fetch("""
        SELECT work_day, tg_user_id, tg_name, checkin_at, checkout_at,
               pee_count, pee_min, poop_count, poop_min,
               meal_count, meal_min, smoke_count, smoke_min
        FROM shift_summary
        WHERE chat_id=$1 AND work_day BETWEEN $2 AND $3
        ORDER BY work_day ASC, tg_user_id ASC
        """, chat_id, start_day, end_day)


# =========================
# 指令：/start /export
# =========================
@dp.message(CommandStart())
async def start_cmd(message: Message):
    if message.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.reply(
            "✅ 打卡机器人已启用（越南时间 UTC+7）\n\n"
            "按钮：上班 / 下班 / 吃饭 / 小便 / 大便 / 抽烟 / 回来\n"
            "规则：不限制什么时候打卡；未上班不能休息；下班后不能再开始休息；进行中必须先回来。\n"
            "统计口径：越南自然日 00:00~23:59。\n\n"
            "导出（仅管理员）：\n"
            "/export 2026-02-05\n"
            "/export 2026-02-01 2026-02-05",
            reply_markup=KB
        )
    else:
        await message.reply("请把机器人拉进群使用。", reply_markup=KB)


@dp.message(Command("export"))
async def export_cmd(message: Message):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return await message.reply("请在群里导出。")

    # ✅ 只允许管理员导出（你要的）
    if not ADMIN_IDS:
        return await message.reply("未配置 ADMIN_IDS，当前禁止导出。请在环境变量设置 ADMIN_IDS=xxx,yyy")
    if message.from_user.id not in ADMIN_IDS:
        return await message.reply("你没有导出权限。")

    parts = (message.text or "").split()
    start_day = end_day = work_day(now_vn())

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
        d1 = parse_d(parts[1])
        d2 = parse_d(parts[2])
        if not d1 or not d2:
            return await message.reply("格式：/export 2026-02-01 2026-02-05")
        start_day, end_day = (d1, d2) if d1 <= d2 else (d2, d1)

    wait = await message.reply("⏳ 正在导出请稍等…")
    try:
        rows = await fetch_export(message.chat.id, start_day, end_day)

        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow([
            "日期(越南)",
            "用户ID",
            "用户名",
            "上班时间(越南)",
            "下班时间(越南)",
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
            checkin_text = ci.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S") if ci else ""
            checkout_text = co.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S") if co else ""

            w.writerow([
                str(r["work_day"]),
                uid_text,
                name_text,
                checkin_text,
                checkout_text,
                int(r["pee_count"]), int(r["pee_min"]),
                int(r["poop_count"]), int(r["poop_min"]),
                int(r["meal_count"]), int(r["meal_min"]),
                int(r["smoke_count"]), int(r["smoke_min"]),
            ])

        data = buf.getvalue().encode("utf-8-sig")
        filename = f"打卡汇总_{message.chat.id}_{start_day}_{end_day}.csv"
        doc = BufferedInputFile(data, filename=filename)
        await message.answer_document(doc)
        await wait.edit_text("✅ 导出完成")
    except Exception as e:
        await wait.edit_text(f"❌ 导出失败：{type(e).__name__}: {e}")


# =========================
# 群消息：打卡入口
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
    wd = work_day(now)

    await ensure_summary_row(chat_id, tg_user_id, tg_name, wd)

    active = await get_active(chat_id, tg_user_id)

    # 如果有进行中的休息，除了 back 之外一律禁止
    if active and kind != "back":
        return await message.reply("⚠️ 你当前还有进行中的状态，请先点【回来】再继续。", reply_markup=KB)

    # 1) 回来：结算一次休息，并删除过程消息
    if kind == "back":
        if not active:
            return await message.reply("你当前没有进行中的记录。", reply_markup=KB)

        act = await clear_active(chat_id, tg_user_id)
        used_min = int(max(0, (now - act["start_at"]).total_seconds() // 60))
        bk = act["kind"]
        act_wd = act["work_day"]

        # 累加汇总
        await ensure_summary_row(chat_id, tg_user_id, tg_name, act_wd)
        await add_break_result(chat_id, tg_user_id, act_wd, bk, used_min)

        used_cnt = await get_kind_count(chat_id, tg_user_id, act_wd, bk)
        limit = DAILY_LIMITS.get(bk, 999)
        left = max(0, limit - used_cnt)

        # 删除过程消息：开始那条 + 机器人两条 + 用户回来这条
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
            f"今日（{act_wd}）已用 {used_cnt}/{limit} 次，剩余 {left} 次。",
            reply_markup=KB,
            parse_mode=ParseMode.HTML
        )

    # 2) 上班：不限制时间
    if kind == "checkin":
        exist = await get_checkin_time(chat_id, tg_user_id, wd)
        if exist:
            return await message.reply("⛔️ 今天已经打过上班了。", reply_markup=KB)

        await set_checkin(chat_id, tg_user_id, wd, now)
        return await message.answer(
            f"✅ {mention} 上班打卡成功（越南时间）：{now.astimezone(TZ).strftime('%Y-%m-%d %H:%M:%S')}",
            reply_markup=KB,
            parse_mode=ParseMode.HTML
        )

    # 3) 下班：不限制时间，但要求先上班 + 不能重复下班
    if kind == "checkout":
        checkin_at = await get_checkin_time(chat_id, tg_user_id, wd)
        if not checkin_at:
            return await message.reply("⛔️ 你今天还没打上班，不能下班。", reply_markup=KB)

        checkout_at = await get_checkout_time(chat_id, tg_user_id, wd)
        if checkout_at:
            return await message.reply("⛔️ 今天已经打过下班了。", reply_markup=KB)

        # 这里 active 已经在上面拦截了（有休息进行中必须先回来）
        await set_checkout(chat_id, tg_user_id, wd, now)
        used_min = int(max(0, (now - checkin_at).total_seconds() // 60))
        return await message.answer(
            f"✅ {mention} 下班打卡成功（越南时间）：{now.astimezone(TZ).strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"⏱ 今日在岗时长（从上班到下班）：{used_min} 分钟（{used_min/60:.2f} 小时）",
            reply_markup=KB,
            parse_mode=ParseMode.HTML
        )

    # 4) 其它休息类型：必须先上班 + 不能下班后再开始
    checkin_at = await get_checkin_time(chat_id, tg_user_id, wd)
    if not checkin_at:
        return await message.reply(
            f"⛔️ 你还没打上班（{wd}），不能进行【{KIND_CN.get(kind, kind)}】。\n请先点【上班】。",
            reply_markup=KB
        )

    checkout_at = await get_checkout_time(chat_id, tg_user_id, wd)
    if checkout_at:
        return await message.reply(
            f"⛔️ 你今天已经下班（{wd}），不能再开始【{KIND_CN.get(kind, kind)}】。",
            reply_markup=KB
        )

    # 5) 次数限制
    used_cnt = await get_kind_count(chat_id, tg_user_id, wd, kind)
    limit = DAILY_LIMITS.get(kind, 999)
    if used_cnt >= limit:
        return await message.reply(
            f"⛔️ 今日（{wd}）【{KIND_CN.get(kind, kind)}】次数已满：{used_cnt}/{limit}。",
            reply_markup=KB
        )

    # 6) 开始一次休息：等回来统一删消息
    minutes = DEFAULT_MINUTES.get(kind, 10)
    deadline = (now + timedelta(minutes=minutes)).astimezone(TZ).strftime("%H:%M")

    msg1 = await message.answer(
        f"📝 {mention} 已记录：{KIND_CN.get(kind, kind)}（第 {used_cnt + 1}/{limit} 次）",
        reply_markup=KB,
        parse_mode=ParseMode.HTML
    )
    msg2 = await message.answer(
        f"⏰ {mention} 请在 {deadline} 前回来（提示值 {minutes} 分钟）。\n"
        f"回来请点【回来】或发：回 / back / 1 / 结束",
        reply_markup=KB,
        parse_mode=ParseMode.HTML
    )

    await set_active(
        chat_id, tg_user_id, wd, kind, now,
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
