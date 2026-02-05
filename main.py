import os
import csv
import io
import asyncio
from datetime import datetime, timedelta, timezone, date
from typing import Optional, Tuple

import asyncpg
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
)
from aiogram.enums import ChatType
from aiogram.types.input_file import BufferedInputFile

# =========================
# 时区：斯里兰卡 GMT+5:30
# =========================
SL_TZ = timezone(timedelta(hours=5, minutes=30))

def now_sl() -> datetime:
    return datetime.now(tz=SL_TZ)

def work_date_sl(dt: datetime) -> date:
    return dt.astimezone(SL_TZ).date()

def parse_date(s: str) -> Optional[date]:
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None

# =========================
# 关键词归一化
# =========================
KIND_ALIASES = {
    "上班": "checkin",
    "/上班": "checkin",
    "开工": "checkin",
    "in": "checkin",

    "吃饭": "meal",
    "eat": "meal",

    "抽烟": "smoke",

    "小便": "pee",
    "尿": "pee",
    "尿尿": "pee",

    "大便": "poop",
    "拉屎": "poop",
    "屎": "poop",
    "便便": "poop",

    "回": "back",
    "回来": "back",
    "/back": "back",
    "1": "back",
    "结束": "back",
}

# 每天最多次数
DAILY_LIMITS = {
    "checkin": 1,   # 每天上班只记一次（严格）
    "meal": 3,
    "smoke": 5,
    "pee": 3,
    "poop": 2,
}

# 默认时长（分钟）
DEFAULT_MINUTES = {
    "meal": 30,
    "smoke": 10,
    "pee": 6,
    "poop": 15,
}

KIND_CN = {
    "checkin": "上班",
    "meal": "吃饭",
    "smoke": "抽烟",
    "pee": "小便",
    "poop": "大便",
}

# =========================
# 键盘（像你截图那种按钮）
# =========================
KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="上班"), KeyboardButton(text="吃饭"), KeyboardButton(text="小便")],
        [KeyboardButton(text="大便"), KeyboardButton(text="抽烟"), KeyboardButton(text="回来")],
    ],
    resize_keyboard=True,
    one_time_keyboard=False,
)

# =========================
# 环境变量
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "").strip()

def parse_admin_ids(raw: str):
    out = set()
    if not raw:
        return out
    for x in raw.split(","):
        x = x.strip()
        if x.isdigit():
            out.add(int(x))
    return out

ADMIN_IDS = parse_admin_ids(ADMIN_IDS_RAW)

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is empty")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is empty")

bot = Bot(BOT_TOKEN)
dp = Dispatcher()
pool: asyncpg.Pool | None = None

# =========================
# DB 初始化（自动补字段）
# =========================
async def db_init():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)

    async with pool.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS groups (
            chat_id BIGINT PRIMARY KEY,
            title TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)

        # ✅ 共用号：绑定多个“身份”
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS user_identities (
            chat_id BIGINT NOT NULL,
            tg_user_id BIGINT NOT NULL,
            label TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (chat_id, tg_user_id, label)
        );
        """)

        # ✅ 当前使用哪个身份（当天/直到切换）
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS user_current_identity (
            chat_id BIGINT NOT NULL,
            tg_user_id BIGINT NOT NULL,
            label TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (chat_id, tg_user_id)
        );
        """)

        # checkins：上班打卡
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS checkins (
            id BIGSERIAL PRIMARY KEY,
            chat_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            username TEXT,
            full_name TEXT,
            actor TEXT,                 -- ✅ 记录“当前身份”
            checkin_at TIMESTAMPTZ NOT NULL,
            work_date DATE NOT NULL
        );
        """)
        # 如果旧表没有 actor，补上
        await conn.execute("ALTER TABLE checkins ADD COLUMN IF NOT EXISTS actor TEXT;")

        await conn.execute("CREATE INDEX IF NOT EXISTS idx_checkins_date_user ON checkins(work_date, user_id);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_checkins_chat_date ON checkins(chat_id, work_date);")

        # break_sessions：吃饭/厕所/抽烟
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS break_sessions (
            id BIGSERIAL PRIMARY KEY,
            chat_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            username TEXT,
            full_name TEXT,
            actor TEXT,                 -- ✅ 记录“当前身份”
            kind TEXT NOT NULL,
            start_at TIMESTAMPTZ NOT NULL,
            end_at TIMESTAMPTZ,
            work_date DATE NOT NULL
        );
        """)
        await conn.execute("ALTER TABLE break_sessions ADD COLUMN IF NOT EXISTS actor TEXT;")

        await conn.execute("CREATE INDEX IF NOT EXISTS idx_breaks_active ON break_sessions(chat_id, user_id) WHERE end_at IS NULL;")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_breaks_date_user_kind ON break_sessions(work_date, user_id, kind);")

        # active_notifs：开始两条消息 id，回来删掉
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS active_notifs (
            chat_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            break_id BIGINT NOT NULL,
            msg_start_id BIGINT,
            msg_tip_id BIGINT,
            PRIMARY KEY (chat_id, user_id)
        );
        """)

# =========================
# DB 工具函数
# =========================
async def upsert_group(chat_id: int, title: str | None):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO groups(chat_id, title)
            VALUES($1, $2)
            ON CONFLICT(chat_id) DO UPDATE SET title=EXCLUDED.title
            """,
            chat_id, title or ""
        )

async def get_active_break(chat_id: int, user_id: int):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            """
            SELECT id, kind, start_at, work_date
            FROM break_sessions
            WHERE chat_id=$1 AND user_id=$2 AND end_at IS NULL
            ORDER BY start_at DESC
            LIMIT 1
            """,
            chat_id, user_id
        )

async def count_today(conn, chat_id: int, user_id: int, kind: str, wd: date) -> int:
    if kind == "checkin":
        return await conn.fetchval(
            "SELECT COUNT(*) FROM checkins WHERE chat_id=$1 AND user_id=$2 AND work_date=$3",
            chat_id, user_id, wd
        )
    return await conn.fetchval(
        "SELECT COUNT(*) FROM break_sessions WHERE chat_id=$1 AND user_id=$2 AND kind=$3 AND work_date=$4",
        chat_id, user_id, kind, wd
    )

async def save_active_notifs(chat_id: int, user_id: int, break_id: int, msg_start_id: int | None, msg_tip_id: int | None):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO active_notifs(chat_id, user_id, break_id, msg_start_id, msg_tip_id)
            VALUES($1,$2,$3,$4,$5)
            ON CONFLICT(chat_id, user_id) DO UPDATE
            SET break_id=EXCLUDED.break_id,
                msg_start_id=EXCLUDED.msg_start_id,
                msg_tip_id=EXCLUDED.msg_tip_id
            """,
            chat_id, user_id, break_id, msg_start_id, msg_tip_id
        )

async def pop_active_notifs(chat_id: int, user_id: int):
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT break_id, msg_start_id, msg_tip_id FROM active_notifs WHERE chat_id=$1 AND user_id=$2",
            chat_id, user_id
        )
        await conn.execute("DELETE FROM active_notifs WHERE chat_id=$1 AND user_id=$2", chat_id, user_id)
        return row

# ---------- 共用号：身份 ----------
async def identity_list(chat_id: int, tg_user_id: int) -> list[str]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT label FROM user_identities WHERE chat_id=$1 AND tg_user_id=$2 ORDER BY label",
            chat_id, tg_user_id
        )
        return [r["label"] for r in rows]

async def identity_bind(chat_id: int, tg_user_id: int, label: str):
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO user_identities(chat_id, tg_user_id, label) VALUES($1,$2,$3) ON CONFLICT DO NOTHING",
            chat_id, tg_user_id, label
        )

async def identity_set_current(chat_id: int, tg_user_id: int, label: str):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO user_current_identity(chat_id, tg_user_id, label)
            VALUES($1,$2,$3)
            ON CONFLICT(chat_id, tg_user_id) DO UPDATE
            SET label=EXCLUDED.label, updated_at=NOW()
            """,
            chat_id, tg_user_id, label
        )

async def identity_get_current(chat_id: int, tg_user_id: int) -> Optional[str]:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT label FROM user_current_identity WHERE chat_id=$1 AND tg_user_id=$2",
            chat_id, tg_user_id
        )

async def require_actor(message: Message) -> Tuple[Optional[str], Optional[str]]:
    """
    返回 (actor, error_msg)
    规则：
    - 如果这个 tg_user 在群里绑定了 >=2 个身份，则必须先 /use 选一个
    - 如果只绑定 0/1 个，则可不选（0 就用 full_name；1 就用那个）
    """
    chat_id = message.chat.id
    tg_user_id = message.from_user.id

    labels = await identity_list(chat_id, tg_user_id)
    cur = await identity_get_current(chat_id, tg_user_id)

    if len(labels) >= 2:
        if not cur:
            return None, f"⚠️ 你这个账号绑定了多个身份：{', '.join(labels)}\n请先用：/use 名字\n例如：/use 张三"
        return cur, None

    if len(labels) == 1:
        # 只有一个身份，自动用它
        if cur != labels[0]:
            await identity_set_current(chat_id, tg_user_id, labels[0])
        return labels[0], None

    # 没绑定：就用 Telegram 名字
    return (message.from_user.full_name or "").strip() or "未知", None

# =========================
# /start
# =========================
@dp.message(CommandStart())
async def cmd_start(message: Message):
    if message.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
        await upsert_group(message.chat.id, message.chat.title)
        await message.reply(
            "✅ 已加入群。\n\n"
            "按钮打卡：上班 / 吃饭 / 小便 / 大便 / 抽烟 / 回来\n"
            "规则：进行中必须先回来；超过次数直接拒绝。\n\n"
            "👥 两个人共用一个号：\n"
            "先绑定：/bind 张三  （再 /bind 李四）\n"
            "再选择：/use 张三\n"
            "查看当前：/who\n",
            reply_markup=KB
        )
    else:
        await message.reply("把我拉进群里用。", reply_markup=KB)

# =========================
# /bind /use /who（解决共用号）
# =========================
@dp.message(Command("bind"))
async def cmd_bind(message: Message):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return await message.reply("请在群里绑定。")
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        return await message.reply("用法：/bind 张三")
    label = parts[1].strip()
    await identity_bind(message.chat.id, message.from_user.id, label)
    await message.reply(f"✅ 已绑定身份：{label}\n用 /use {label} 切换当前身份。", reply_markup=KB)

@dp.message(Command("use"))
async def cmd_use(message: Message):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return await message.reply("请在群里使用。")
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        return await message.reply("用法：/use 张三")
    label = parts[1].strip()

    labels = await identity_list(message.chat.id, message.from_user.id)
    if labels and (label not in labels):
        return await message.reply(f"⛔️ 你没有绑定这个身份：{label}\n已绑定：{', '.join(labels)}")

    await identity_set_current(message.chat.id, message.from_user.id, label)
    await message.reply(f"✅ 当前身份已切换为：{label}", reply_markup=KB)

@dp.message(Command("who"))
async def cmd_who(message: Message):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return await message.reply("请在群里查看。")
    cur = await identity_get_current(message.chat.id, message.from_user.id)
    labels = await identity_list(message.chat.id, message.from_user.id)
    if labels:
        return await message.reply(f"👤 当前身份：{cur or '未选择'}\n已绑定：{', '.join(labels)}")
    return await message.reply(f"👤 当前身份：{cur or (message.from_user.full_name or '未绑定')}（未绑定身份时默认用 Telegram 名字）")

# =========================
# 导出 /export（管理员）
# =========================
@dp.message(Command("export"))
async def cmd_export(message: Message):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return await message.reply("请在群里导出。")

    if ADMIN_IDS and (message.from_user.id not in ADMIN_IDS):
        return await message.reply("你没有导出权限。")

    # 参数：/export  或 /export 2026-02-05 或 /export 2026-02-01 2026-02-05
    parts = (message.text or "").split()
    start_d = end_d = work_date_sl(now_sl())
    if len(parts) == 2:
        d = parse_date(parts[1])
        if not d:
            return await message.reply("日期格式：YYYY-MM-DD")
        start_d = end_d = d
    elif len(parts) >= 3:
        d1 = parse_date(parts[1])
        d2 = parse_date(parts[2])
        if not d1 or not d2:
            return await message.reply("日期格式：/export 2026-02-01 2026-02-05")
        start_d, end_d = (d1, d2) if d1 <= d2 else (d2, d1)

    wait_msg = await message.reply("⏳ 正在导出请稍等…")

    try:
        chat_id = message.chat.id

        async with pool.acquire() as conn:
            checkins = await conn.fetch(
                """
                SELECT user_id, username, full_name, actor, checkin_at, work_date
                FROM checkins
                WHERE chat_id=$1 AND work_date BETWEEN $2 AND $3
                ORDER BY checkin_at ASC
                """,
                chat_id, start_d, end_d
            )
            breaks = await conn.fetch(
                """
                SELECT user_id, username, full_name, actor, kind, start_at, end_at, work_date
                FROM break_sessions
                WHERE chat_id=$1 AND work_date BETWEEN $2 AND $3
                ORDER BY start_at ASC
                """,
                chat_id, start_d, end_d
            )

        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["type", "work_date", "tg_user_id", "actor", "username", "full_name", "kind", "start_at", "end_at", "minutes"])

        for r in checkins:
            w.writerow([
                "checkin",
                str(r["work_date"]),
                r["user_id"],
                r["actor"] or "",
                r["username"] or "",
                r["full_name"] or "",
                "上班",
                r["checkin_at"].astimezone(SL_TZ).strftime("%Y-%m-%d %H:%M:%S"),
                "",
                ""
            ])

        for r in breaks:
            s = r["start_at"].astimezone(SL_TZ)
            e = r["end_at"].astimezone(SL_TZ) if r["end_at"] else None
            mins = ""
            if e:
                mins = int((e - s).total_seconds() // 60)
            w.writerow([
                "break",
                str(r["work_date"]),
                r["user_id"],
                r["actor"] or "",
                r["username"] or "",
                r["full_name"] or "",
                KIND_CN.get(r["kind"], r["kind"]),
                s.strftime("%Y-%m-%d %H:%M:%S"),
                e.strftime("%Y-%m-%d %H:%M:%S") if e else "",
                mins
            ])

        data = buf.getvalue().encode("utf-8-sig")
        filename = f"attendance_{chat_id}_{start_d}_{end_d}.csv"

        doc = BufferedInputFile(data, filename=filename)
        await message.answer_document(doc)
        await wait_msg.edit_text("✅ 导出完成")

    except Exception as e:
        # 关键：失败也要告诉你，不会一直“正在导出”
        try:
            await wait_msg.edit_text(f"❌ 导出失败：{type(e).__name__}: {e}")
        except Exception:
            pass

# =========================
# 群消息：打卡逻辑
# =========================
@dp.message(F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}) & F.text)
async def on_group_text(message: Message):
    raw = (message.text or "").strip()
    kind = KIND_ALIASES.get(raw)
    if not kind:
        return

    chat_id = message.chat.id
    user_id = message.from_user.id
    username = message.from_user.username or ""
    full_name = (message.from_user.full_name or "").strip()

    # ✅ 共用号：要求先选身份
    actor, err = await require_actor(message)
    if err:
        return await message.reply(err, reply_markup=KB)

    now = now_sl()
    wd = work_date_sl(now)

    # 先查：是否有进行中的 break
    active = await get_active_break(chat_id, user_id)

    # 规则：有进行中必须先回来
    if kind != "back" and active:
        kcn = KIND_CN.get(active["kind"], active["kind"])
        start = active["start_at"].astimezone(SL_TZ).strftime("%H:%M")
        return await message.reply(
            f"⚠️ 你现在还在【{kcn}】中（开始于 {start}）。\n请先点【回来】或发 /back /回 /1 /结束。",
            reply_markup=KB
        )

    # back：结束进行中
    if kind == "back":
        if not active:
            return await message.reply("你当前没有进行中的状态。", reply_markup=KB)

        break_id = int(active["id"])
        bkind = active["kind"]
        started = active["start_at"].astimezone(SL_TZ)

        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE break_sessions SET end_at=$1 WHERE id=$2 AND end_at IS NULL",
                now, break_id
            )
            used = int((now - active["start_at"]).total_seconds() // 60)
            used = max(0, used)

            used_cnt = await count_today(conn, chat_id, user_id, bkind, wd)
            limit = DAILY_LIMITS.get(bkind, 999)
            left = max(0, limit - used_cnt)

        # 删除“开始那两条提示消息”
        notif = await pop_active_notifs(chat_id, user_id)
        if notif:
            for mid in [notif["msg_start_id"], notif["msg_tip_id"]]:
                if mid:
                    try:
                        await bot.delete_message(chat_id, int(mid))
                    except Exception:
                        pass

        kcn = KIND_CN.get(bkind, bkind)
        return await message.reply(
            f"✅ {actor} 已回来：本次【{kcn}】用时 {used} 分钟。\n"
            f"今天【{kcn}】已用 {used_cnt}/{limit} 次，还剩 {left} 次。",
            reply_markup=KB
        )

    # 下面：kind 是 checkin / meal / smoke / pee / poop
    async with pool.acquire() as conn:
        used_cnt = await count_today(conn, chat_id, user_id, kind, wd)
        limit = DAILY_LIMITS.get(kind, 999)

        # 超过次数拒绝
        if used_cnt >= limit:
            name = KIND_CN.get(kind, kind)
            return await message.reply(
                f"⛔️ {actor} 今天【{name}】次数已满：{used_cnt}/{limit}。\n不能再打这个卡了。",
                reply_markup=KB
            )

        # 上班：写 checkins
        if kind == "checkin":
            await conn.execute(
                """
                INSERT INTO checkins(chat_id, user_id, username, full_name, actor, checkin_at, work_date)
                VALUES($1,$2,$3,$4,$5,$6,$7)
                """,
                chat_id, user_id, username, full_name, actor, now, wd
            )
            new_cnt = used_cnt + 1
            return await message.reply(
                f"✅ {actor} 上班打卡成功，辛苦辛苦~\n今日上班打卡：{new_cnt}/{limit}",
                reply_markup=KB
            )

        # break：开始
        minutes = DEFAULT_MINUTES.get(kind, 10)
        deadline = (now + timedelta(minutes=minutes)).astimezone(SL_TZ).strftime("%H:%M")

        row = await conn.fetchrow(
            """
            INSERT INTO break_sessions(chat_id, user_id, username, full_name, actor, kind, start_at, work_date)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8)
            RETURNING id
            """,
            chat_id, user_id, username, full_name, actor, kind, now, wd
        )
        break_id = int(row["id"])
        new_cnt = used_cnt + 1
        left = max(0, limit - new_cnt)

    # 休息开始：发两条（回来时删）
    name = KIND_CN.get(kind, kind)
    msg1 = await message.reply(
        f"📝 已记录：{actor} {name}（第 {new_cnt}/{limit} 次）。",
        reply_markup=KB
    )
    msg2 = await message.reply(
        f"⏰ 请在 {deadline} 前回来。\n"
        f"回来请点【回来】或发 /back /回 /1 /结束。\n"
        f"（今日还剩 {left} 次 {name}）",
        reply_markup=KB
    )
    await save_active_notifs(chat_id, user_id, break_id, msg1.message_id, msg2.message_id)

# =========================
# 启动
# =========================
async def main():
    await db_init()
    # polling 前清 webhook（不清就可能“没反应”）
    await bot.delete_webhook(drop_pending_updates=True)
    print("[bot] polling started")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
