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
# 规则（按越南自然日）
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

# ✅ 重要：全部用 /命令，确保隐私模式也能收到
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

# 也支持中文/英文纯文本（如果隐私模式开着可能收不到，但不影响 /命令）
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


# ✅ 键盘：全部是 /命令（点了必触发）
KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="/in"), KeyboardButton(text="/out")],
        [KeyboardButton(text="/meal"), KeyboardButton(text="/pee"), KeyboardButton(text="/poop")],
        [KeyboardButton(text="/smoke"), KeyboardButton(text="/back")],
        [KeyboardButton(text="/export")],
    ],
    resize_keyboard=True
)


# =========================
# 时间：越南自然日
# =========================
def now_vn() -> datetime:
    return datetime.now(tz=TZ)


def day_vn(dt: datetime) -> date:
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


def norm(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", "", s)
    return s


# =========================
# DB：用新表名，避免旧库冲突
# =========================
TABLE_SUM = "shift_summary_vn_v2"
TABLE_ACT = "active_session_vn_v2"


async def db_init():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)

    async with pool.acquire() as conn:
        await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_SUM} (
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

        await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_ACT} (
            chat_id BIGINT NOT NULL,
            tg_user_id BIGINT NOT NULL,
            work_day DATE NOT NULL,
            kind TEXT NOT NULL,              -- pee/poop/meal/smoke
            start_at TIMESTAMPTZ NOT NULL,
            start_msg BIGINT,
            msg1 BIGINT,
            msg2 BIGINT,
            PRIMARY KEY (chat_id, tg_user_id)
        );
        """)

        await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_sum_day_v2 ON {TABLE_SUM}(chat_id, work_day);")


# =========================
# DB 工具
# =========================
async def ensure_summary_row(chat_id: int, tg_user_id: int, tg_name: str, wd: date):
    async with pool.acquire() as conn:
        await conn.execute(f"""
        INSERT INTO {TABLE_SUM}(chat_id, tg_user_id, tg_name, work_day)
        VALUES($1,$2,$3,$4)
        ON CONFLICT(chat_id, tg_user_id, work_day)
        DO UPDATE SET tg_name=EXCLUDED.tg_name, updated_at=NOW()
        """, chat_id, tg_user_id, tg_name, wd)


async def get_checkin(chat_id: int, tg_user_id: int, wd: date) -> Optional[datetime]:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            f"SELECT checkin_at FROM {TABLE_SUM} WHERE chat_id=$1 AND tg_user_id=$2 AND work_day=$3",
            chat_id, tg_user_id, wd
        )


async def get_checkout(chat_id: int, tg_user_id: int, wd: date) -> Optional[datetime]:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            f"SELECT checkout_at FROM {TABLE_SUM} WHERE chat_id=$1 AND tg_user_id=$2 AND work_day=$3",
            chat_id, tg_user_id, wd
        )


async def set_checkin(chat_id: int, tg_user_id: int, wd: date, t: datetime):
    async with pool.acquire() as conn:
        await conn.execute(
            f"UPDATE {TABLE_SUM} SET checkin_at=$1, updated_at=NOW() WHERE chat_id=$2 AND tg_user_id=$3 AND work_day=$4",
            t, chat_id, tg_user_id, wd
        )


async def set_checkout(chat_id: int, tg_user_id: int, wd: date, t: datetime):
    async with pool.acquire() as conn:
        await conn.execute(
            f"UPDATE {TABLE_SUM} SET checkout_at=$1, updated_at=NOW() WHERE chat_id=$2 AND tg_user_id=$3 AND work_day=$4",
            t, chat_id, tg_user_id, wd
        )


async def get_kind_count(chat_id: int, tg_user_id: int, wd: date, kind: str) -> int:
    col = f"{kind}_count"
    async with pool.acquire() as conn:
        v = await conn.fetchval(
            f"SELECT {col} FROM {TABLE_SUM} WHERE chat_id=$1 AND tg_user_id=$2 AND work_day=$3",
            chat_id, tg_user_id, wd
        )
    return int(v or 0)


async def add_break(chat_id: int, tg_user_id: int, wd: date, kind: str, used_min: int):
    count_col = f"{kind}_count"
    min_col = f"{kind}_min"
    async with pool.acquire() as conn:
        await conn.execute(f"""
        UPDATE {TABLE_SUM}
        SET {count_col} = {count_col} + 1,
            {min_col}   = {min_col} + $1,
            updated_at  = NOW()
        WHERE chat_id=$2 AND tg_user_id=$3 AND work_day=$4
        """, used_min, chat_id, tg_user_id, wd)


async def get_active(chat_id: int, tg_user_id: int):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            f"SELECT * FROM {TABLE_ACT} WHERE chat_id=$1 AND tg_user_id=$2",
            chat_id, tg_user_id
        )


async def set_active(chat_id: int, tg_user_id: int, wd: date, kind: str,
                     start_at: datetime, start_msg: int, msg1: int, msg2: int):
    async with pool.acquire() as conn:
        await conn.execute(f"""
        INSERT INTO {TABLE_ACT}(chat_id, tg_user_id, work_day, kind, start_at, start_msg, msg1, msg2)
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
            f"SELECT * FROM {TABLE_ACT} WHERE chat_id=$1 AND tg_user_id=$2",
            chat_id, tg_user_id
        )
        await conn.execute(
            f"DELETE FROM {TABLE_ACT} WHERE chat_id=$1 AND tg_user_id=$2",
            chat_id, tg_user_id
        )
        return row


async def fetch_export(chat_id: int, start_day: date, end_day: date):
    async with pool.acquire() as conn:
        return await conn.fetch(f"""
        SELECT work_day, tg_user_id, tg_name, checkin_at, checkout_at,
               pee_count, pee_min, poop_count, poop_min,
               meal_count, meal_min, smoke_count, smoke_min
        FROM {TABLE_SUM}
        WHERE chat_id=$1 AND work_day BETWEEN $2 AND $3
        ORDER BY work_day ASC, tg_user_id ASC
        """, chat_id, start_day, end_day)


# =========================
# /start + /export 指令
# =========================
@dp.message(CommandStart())
async def start_cmd(message: Message):
    if message.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.reply(
            "✅ 打卡机器人已启用（越南时间 UTC+7）\n\n"
            "请用按钮（推荐）或命令：\n"
            "/in 上班  |  /out 下班\n"
            "/meal 吃饭 | /pee 小便 | /poop 大便 | /smoke 抽烟\n"
            "/back 回来（结束本次休息并结算）\n\n"
            "导出（仅管理员）：/export 2026-02-07 或 /export 2026-02-01 2026-02-07\n",
            reply_markup=KB
        )
    else:
        await message.reply("请把机器人拉进群使用。", reply_markup=KB)


@dp.message(Command("export"))
async def export_cmd(message: Message):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return await message.reply("请在群里导出。")

    # ✅ 只允许管理员导出
    if not ADMIN_IDS:
        return await message.reply("未配置 ADMIN_IDS，当前禁止导出。请设置 ADMIN_IDS=xxx,yyy")
    if message.from_user.id not in ADMIN_IDS:
        return await message.reply("你没有导出权限。")

    parts = (message.text or "").split()
    today = day_vn(now_vn())
    start_day = end_day = today

    def parse_d(s: str) -> Optional[date]:
        try:
            return datetime.strptime(s, "%Y-%m-%d").date()
        except Exception:
            return None

    if len(parts) == 2:
        d = parse_d(parts[1])
        if not d:
            return await message.reply("格式：/export 2026-02-07")
        start_day = end_day = d
    elif len(parts) >= 3:
        d1 = parse_d(parts[1])
        d2 = parse_d(parts[2])
        if not d1 or not d2:
            return await message.reply("格式：/export 2026-02-01 2026-02-07")
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
        await message.answer_document(BufferedInputFile(data, filename=filename))
        await wait.edit_text("✅ 导出完成")
    except Exception as e:
        await wait.edit_text(f"❌ 导出失败：{type(e).__name__}: {e}")


# =========================
# 统一处理入口（只要群里发消息）
# =========================
@dp.message(F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}) & F.text)
async def on_group_text(message: Message):
    raw = norm(message.text or "")
    raw_lower = raw.lower()

    # 1) 优先识别 /命令（隐私模式也能收到）
    kind = CMD_ALIASES.get(raw_lower)

    # 2) 其次识别普通文字（隐私模式开着可能收不到，但不影响）
    if not kind:
        kind = TEXT_ALIASES.get(raw) or TEXT_ALIASES.get(raw_lower)

    if not kind:
        return

    chat_id = message.chat.id
    tg_user_id = message.from_user.id
    tg_name = get_tg_name(message)
    mention = mention_html(message)

    now = now_vn()
    wd = day_vn(now)

    # 导出：直接走 /export（只有管理员能出）
    if kind == "export":
        message.text = "/export"
        return await export_cmd(message)

    await ensure_summary_row(chat_id, tg_user_id, tg_name, wd)

    active = await get_active(chat_id, tg_user_id)

    # 有进行中的休息：除 /back 外都拦住
    if active and kind != "back":
        return await message.reply("⚠️ 你当前还有进行中的状态，请先点【/back】再继续。", reply_markup=KB)

    # /back：结束休息并结算
    if kind == "back":
        if not active:
            return await message.reply("你当前没有进行中的记录。", reply_markup=KB)

        act = await clear_active(chat_id, tg_user_id)
        used_min = int(max(0, (now - act["start_at"]).total_seconds() // 60))
        bk = act["kind"]
        act_day = act["work_day"]

        await ensure_summary_row(chat_id, tg_user_id, tg_name, act_day)
        await add_break(chat_id, tg_user_id, act_day, bk, used_min)

        used_cnt = await get_kind_count(chat_id, tg_user_id, act_day, bk)
        limit = DAILY_LIMITS.get(bk, 999)
        left = max(0, limit - used_cnt)

        # 删除过程消息（可删就删，删不了不影响）
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
            f"今日（{act_day}）已用 {used_cnt}/{limit} 次，剩余 {left} 次。",
            reply_markup=KB,
            parse_mode=ParseMode.HTML
        )

    # /in：上班（不限制时间）
    if kind == "checkin":
        exist = await get_checkin(chat_id, tg_user_id, wd)
        if exist:
            return await message.reply("⛔️ 今天已经打过上班了。", reply_markup=KB)

        await set_checkin(chat_id, tg_user_id, wd, now)
        return await message.answer(
            f"✅ {mention} 上班打卡成功（越南时间）：{now.astimezone(TZ).strftime('%Y-%m-%d %H:%M:%S')}",
            reply_markup=KB,
            parse_mode=ParseMode.HTML
        )

    # /out：下班（不限制时间，但必须先上班）
    if kind == "checkout":
        ci = await get_checkin(chat_id, tg_user_id, wd)
        if not ci:
            return await message.reply("⛔️ 你今天还没上班，不能下班。请先点 /in", reply_markup=KB)

        co = await get_checkout(chat_id, tg_user_id, wd)
        if co:
            return await message.reply("⛔️ 今天已经打过下班了。", reply_markup=KB)

        await set_checkout(chat_id, tg_user_id, wd, now)
        used_min = int(max(0, (now - ci).total_seconds() // 60))
        return await message.answer(
            f"✅ {mention} 下班打卡成功（越南时间）：{now.astimezone(TZ).strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"⏱ 今日在岗时长（上班→下班）：{used_min} 分钟（{used_min/60:.2f} 小时）",
            reply_markup=KB,
            parse_mode=ParseMode.HTML
        )

    # 休息：必须先上班，且不能下班后再开始
    ci = await get_checkin(chat_id, tg_user_id, wd)
    if not ci:
        return await message.reply("⛔️ 你今天还没上班，不能休息。请先点 /in", reply_markup=KB)

    co = await get_checkout(chat_id, tg_user_id, wd)
    if co:
        return await message.reply("⛔️ 你今天已经下班，不能再开始休息。", reply_markup=KB)

    # 次数限制
    used_cnt = await get_kind_count(chat_id, tg_user_id, wd, kind)
    limit = DAILY_LIMITS.get(kind, 999)
    if used_cnt >= limit:
        return await message.reply(
            f"⛔️ 今日（{wd}）【{KIND_CN.get(kind, kind)}】次数已满：{used_cnt}/{limit}。",
            reply_markup=KB
        )

    # 开始休息：发两条提示，等 /back 结算并删消息
    minutes = DEFAULT_MINUTES.get(kind, 10)
    deadline = (now + timedelta(minutes=minutes)).astimezone(TZ).strftime("%H:%M")

    msg1 = await message.answer(
        f"📝 {mention} 已记录：{KIND_CN.get(kind, kind)}（第 {used_cnt + 1}/{limit} 次）",
        reply_markup=KB,
        parse_mode=ParseMode.HTML
    )
    msg2 = await message.answer(
        f"⏰ {mention} 请在 {deadline} 前回来（提示值 {minutes} 分钟）。\n"
        f"结束请点【/back】",
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
