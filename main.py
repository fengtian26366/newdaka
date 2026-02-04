import os
import re
import asyncio
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from typing import Optional, Tuple, List

import asyncpg
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, FSInputFile
from aiogram.filters import Command
from dotenv import load_dotenv

from openpyxl import Workbook


# ======================
# 基础配置
# ======================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "").strip()

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN 未设置")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL 未设置")

TZ = ZoneInfo("Asia/Colombo")  # 斯里兰卡

# 管理员（如果没填，就默认所有人可导出）
ADMIN_IDS = set()
if ADMIN_IDS_RAW:
    for x in ADMIN_IDS_RAW.split(","):
        x = x.strip()
        if x.isdigit():
            ADMIN_IDS.add(int(x))

# 你给的规则：次数 + 单次上限分钟
BREAK_RULES = {
    "pee":  {"name": "小便/厕所", "max_times": 3, "limit_min": 6,
             "keywords": ["小便", "尿", "pee", "p", "厕所", "上厕所", "贝所"]},
    "poo":  {"name": "大便", "max_times": 2, "limit_min": 15,
             "keywords": ["大便", "拉屎", "poo", "shit"]},
    "meal": {"name": "吃饭", "max_times": 3, "limit_min": 30,
             "keywords": ["吃饭", "eat", "meal"]},
    "smk":  {"name": "抽烟", "max_times": 5, "limit_min": 10,
             "keywords": ["抽烟", "抽", "smoke", "smk"]},
}

# 上班关键词（群里直接发）
CHECKIN_KEYWORDS = ["上班", "开工", "上工", "in", "start", "签到", "到岗"]
# 回来关键词
BACK_KEYWORDS = ["回", "回来", "back", "return", "1", "结束", "结束了"]


def now_tz() -> datetime:
    return datetime.now(TZ)


def date_str(d: date) -> str:
    return d.isoformat()


def norm_text(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", "", s)
    return s


def is_admin(user_id: int) -> bool:
    if not ADMIN_IDS:
        return True
    return user_id in ADMIN_IDS


def is_checkin(text: str) -> bool:
    t = norm_text(text)
    return any(t == norm_text(k) or norm_text(k) in t for k in CHECKIN_KEYWORDS)


def is_back(text: str) -> bool:
    t = norm_text(text)
    return any(t == norm_text(k) or norm_text(k) in t for k in BACK_KEYWORDS)


def match_break_kind(text: str) -> Optional[str]:
    # ✅ 包含匹配（你发“去吃饭”“上厕所”也能识别）
    t = norm_text(text)
    for kind, rule in BREAK_RULES.items():
        for kw in rule["keywords"]:
            if norm_text(kw) in t:
                return kind
    return None


def nice_checkin_reply() -> str:
    # 简单“欣慰”文案（你想要更多可以再加）
    samples = [
        "✅ 打卡成功，辛苦辛苦~",
        "✅ 已记录上班，加油！",
        "✅ 收到，上班打卡完成。",
        "✅ 记上了，今天也稳稳的。",
    ]
    # 不用随机也行，这里简单按秒取
    idx = int(datetime.utcnow().timestamp()) % len(samples)
    return samples[idx]


def deadline_text(start_at: datetime, limit_min: int) -> str:
    dl = start_at + timedelta(minutes=limit_min)
    return dl.strftime("%H:%M")


# ======================
# 数据库
# ======================
pool: Optional[asyncpg.Pool] = None

async def db_init():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)

    async with pool.acquire() as conn:
        # 群表
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS groups (
            chat_id BIGINT PRIMARY KEY,
            title TEXT,
            added_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)

        # 上班打卡（只记录上班）
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS checkins (
            id BIGSERIAL PRIMARY KEY,
            chat_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            username TEXT,
            full_name TEXT,
            checkin_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            work_date DATE NOT NULL
        );
        """)

        # 外出记录（一次一条）
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS break_sessions (
            id BIGSERIAL PRIMARY KEY,
            chat_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            username TEXT,
            full_name TEXT,
            kind TEXT NOT NULL,
            start_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            end_at TIMESTAMPTZ,
            msg_id1 BIGINT,
            msg_id2 BIGINT,
            work_date DATE NOT NULL
        );
        """)

        # ✅ 自动补字段（避免你现在遇到的 end_at 不存在）
        await conn.execute("ALTER TABLE break_sessions ADD COLUMN IF NOT EXISTS end_at TIMESTAMPTZ;")
        await conn.execute("ALTER TABLE break_sessions ADD COLUMN IF NOT EXISTS msg_id1 BIGINT;")
        await conn.execute("ALTER TABLE break_sessions ADD COLUMN IF NOT EXISTS msg_id2 BIGINT;")

        # 索引
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_checkins_date_user ON checkins(work_date, user_id);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_breaks_date_user_kind ON break_sessions(work_date, user_id, kind);")


async def upsert_group(chat_id: int, title: str):
    async with pool.acquire() as conn:
        await conn.execute("""
        INSERT INTO groups(chat_id, title) VALUES($1, $2)
        ON CONFLICT(chat_id) DO UPDATE SET title=EXCLUDED.title
        """, chat_id, title)


async def add_checkin(chat_id: int, user_id: int, username: str, full_name: str, dt: datetime):
    wd = dt.date()
    async with pool.acquire() as conn:
        await conn.execute("""
        INSERT INTO checkins(chat_id, user_id, username, full_name, checkin_at, work_date)
        VALUES($1,$2,$3,$4,$5,$6)
        """, chat_id, user_id, username, full_name, dt, wd)


async def get_active_session(chat_id: int, user_id: int) -> Optional[asyncpg.Record]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
        SELECT * FROM break_sessions
        WHERE chat_id=$1 AND user_id=$2 AND end_at IS NULL
        ORDER BY start_at DESC
        LIMIT 1
        """, chat_id, user_id)
        return row


async def count_break_used(chat_id: int, user_id: int, wd: date, kind: str) -> int:
    async with pool.acquire() as conn:
        n = await conn.fetchval("""
        SELECT COUNT(1) FROM break_sessions
        WHERE chat_id=$1 AND user_id=$2 AND work_date=$3 AND kind=$4
        """, chat_id, user_id, wd, kind)
        return int(n or 0)


async def start_break(chat_id: int, user_id: int, username: str, full_name: str, kind: str, dt: datetime) -> int:
    wd = dt.date()
    async with pool.acquire() as conn:
        rid = await conn.fetchval("""
        INSERT INTO break_sessions(chat_id, user_id, username, full_name, kind, start_at, work_date)
        VALUES($1,$2,$3,$4,$5,$6,$7)
        RETURNING id
        """, chat_id, user_id, username, full_name, kind, dt, wd)
        return int(rid)


async def set_break_msgs(session_id: int, msg1: int, msg2: int):
    async with pool.acquire() as conn:
        await conn.execute("""
        UPDATE break_sessions SET msg_id1=$1, msg_id2=$2 WHERE id=$3
        """, msg1, msg2, session_id)


async def end_break(session_id: int, end_dt: datetime):
    async with pool.acquire() as conn:
        await conn.execute("""
        UPDATE break_sessions SET end_at=$1 WHERE id=$2
        """, end_dt, session_id)


async def fetch_break_by_id(session_id: int) -> Optional[asyncpg.Record]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM break_sessions WHERE id=$1", session_id)
        return row


async def list_groups() -> List[asyncpg.Record]:
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT chat_id, title, added_at FROM groups ORDER BY added_at DESC")
        return list(rows)


# ======================
# 导出 XLSX
# ======================
async def export_xlsx(month: Optional[str] = None) -> str:
    """
    month: "YYYY-MM" 或 None(默认本月)
    返回生成的文件路径
    """
    dt = now_tz()
    if month:
        y, m = month.split("-")
        y = int(y); m = int(m)
        start = date(y, m, 1)
    else:
        start = date(dt.year, dt.month, 1)

    # end = 下月1号
    if start.month == 12:
        end = date(start.year + 1, 1, 1)
    else:
        end = date(start.year, start.month + 1, 1)

    async with pool.acquire() as conn:
        checkins = await conn.fetch("""
            SELECT chat_id, user_id, username, full_name, checkin_at, work_date
            FROM checkins
            WHERE work_date >= $1 AND work_date < $2
            ORDER BY work_date ASC, checkin_at ASC
        """, start, end)

        breaks = await conn.fetch("""
            SELECT chat_id, user_id, username, full_name, kind, start_at, end_at, work_date
            FROM break_sessions
            WHERE work_date >= $1 AND work_date < $2
            ORDER BY work_date ASC, start_at ASC
        """, start, end)

    # 统计：每天是否打卡
    # key=(chat_id,user_id,work_date) => first_checkin_time
    first_checkin = {}
    for r in checkins:
        key = (int(r["chat_id"]), int(r["user_id"]), r["work_date"])
        if key not in first_checkin:
            first_checkin[key] = r["checkin_at"]

    # 统计 break 次数 & 本次时长
    wb = Workbook()

    ws1 = wb.active
    ws1.title = "打卡"
    ws1.append(["日期", "群ID", "用户ID", "用户名", "昵称", "最早上班时间", "是否缺上班打卡(是/否)"])

    # 我们以“出现过的人”为基准（本月有打卡/有外出的人）
    people_keys = set()
    for r in checkins:
        people_keys.add((int(r["chat_id"]), int(r["user_id"]), r["username"], r["full_name"]))
    for r in breaks:
        people_keys.add((int(r["chat_id"]), int(r["user_id"]), r["username"], r["full_name"]))

    # 本月所有日期
    days = []
    d = start
    while d < end:
        days.append(d)
        d += timedelta(days=1)

    for (chat_id, user_id, username, full_name) in sorted(people_keys, key=lambda x: (x[0], x[1])):
        for wd in days:
            key = (chat_id, user_id, wd)
            t0 = first_checkin.get(key)
            ws1.append([
                wd.isoformat(),
                chat_id,
                user_id,
                username or "",
                full_name or "",
                t0.astimezone(TZ).strftime("%H:%M:%S") if t0 else "",
                "是" if not t0 else "否"
            ])

    ws2 = wb.create_sheet("外出明细")
    ws2.append(["日期", "群ID", "用户ID", "用户名", "昵称", "类型", "开始", "结束", "用时(分钟)", "是否超时"])

    for r in breaks:
        s = r["start_at"].astimezone(TZ)
        e = r["end_at"].astimezone(TZ) if r["end_at"] else None
        kind = r["kind"]
        limit = BREAK_RULES.get(kind, {}).get("limit_min", 0)
        used_min = ""
        overtime = ""
        if e:
            used = (e - s).total_seconds() / 60.0
            used_min = round(used, 2)
            overtime = "是" if (limit and used > limit) else "否"

        ws2.append([
            r["work_date"].isoformat(),
            int(r["chat_id"]),
            int(r["user_id"]),
            r["username"] or "",
            r["full_name"] or "",
            BREAK_RULES.get(kind, {}).get("name", kind),
            s.strftime("%H:%M:%S"),
            e.strftime("%H:%M:%S") if e else "",
            used_min,
            overtime
        ])

    path = f"/mnt/data/打卡导出_{start.strftime('%Y-%m')}.xlsx"
    wb.save(path)
    return path


# ======================
# Bot / Router
# ======================
bot = Bot(BOT_TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)


@router.message(Command("start"))
async def cmd_start(msg: Message):
    # 群：加入群记录
    if msg.chat.type in ("group", "supergroup"):
        await upsert_group(msg.chat.id, msg.chat.title or "")
        await msg.reply("✅ 已加入群。直接发 上班/吃饭/eat/抽烟/厕所/小便/大便 即可记录；回来发：回/back/1/结束。")
        return

    # 私聊：说明
    await msg.reply(
        "✅ 兰卡打卡机器人\n\n"
        "群里：直接发【上班/开工/in】=上班打卡；发【吃饭/eat/抽烟/厕所/小便/大便】=开始外出；发【回/back/1/结束】=回来结算。\n\n"
        "私聊命令：\n"
        "1) /mygroups 查看加入的群\n"
        "2) /export 或 /export 2026-02 导出xlsx\n"
    )


@router.message(Command("mygroups"))
async def cmd_mygroups(msg: Message):
    if msg.chat.type != "private":
        return
    rows = await list_groups()
    if not rows:
        await msg.reply("暂无群记录（先把机器人拉进群，然后在群里发 /start）。")
        return

    lines = ["✅ 已加入的群："]
    for r in rows[:50]:
        lines.append(f"- {r['title'] or '(无标题)'} | {int(r['chat_id'])}")
    await msg.reply("\n".join(lines))


@router.message(Command("export"))
async def cmd_export(msg: Message):
    if msg.chat.type != "private":
        return
    if not is_admin(msg.from_user.id):
        await msg.reply("❌ 你没有导出权限。")
        return

    # /export 或 /export 2026-02
    parts = (msg.text or "").split()
    month = None
    if len(parts) >= 2:
        month = parts[1].strip()
        if not re.match(r"^\d{4}-\d{2}$", month):
            await msg.reply("格式不对：/export 或 /export 2026-02")
            return

    await msg.reply("正在导出，请稍等…")
    path = await export_xlsx(month)
    await msg.reply_document(FSInputFile(path))


@router.message(F.chat.type.in_({"group", "supergroup"}) & F.text)
async def on_group_text(msg: Message):
    """
    群文本入口：上班 / 开始外出 / 回来结算
    """
    text = (msg.text or "").strip()
    user = msg.from_user
    if not user:
        return

    chat_id = msg.chat.id
    user_id = user.id
    username = user.username or ""
    full_name = (user.full_name or "").strip()

    dt = now_tz()

    # 1) 回来：结束外出
    if is_back(text):
        active = await get_active_session(chat_id, user_id)
        if not active:
            await msg.reply("⚠️ 你当前没有进行中的外出记录。")
            return

        session_id = int(active["id"])
        kind = active["kind"]
        start_at = active["start_at"].astimezone(TZ)
        limit_min = BREAK_RULES.get(kind, {}).get("limit_min", 0)
        max_times = BREAK_RULES.get(kind, {}).get("max_times", 0)

        await end_break(session_id, dt)
        row = await fetch_break_by_id(session_id)
        end_at = row["end_at"].astimezone(TZ) if row and row["end_at"] else dt

        used_min = (end_at - start_at).total_seconds() / 60.0
        used_min_round = round(used_min, 2)

        wd = row["work_date"]
        used_times = await count_break_used(chat_id, user_id, wd, kind)  # 结束后已算进去
        remain = max(0, max_times - used_times)

        overtime = (limit_min and used_min > limit_min)

        # 删除开始外出时机器人发的两条提示
        try:
            if row["msg_id1"]:
                await bot.delete_message(chat_id, int(row["msg_id1"]))
            if row["msg_id2"]:
                await bot.delete_message(chat_id, int(row["msg_id2"]))
        except Exception:
            pass

        kind_name = BREAK_RULES.get(kind, {}).get("name", kind)
        tip = "⚠️ 本次已超时。" if overtime else "✅ 本次未超时。"
        await msg.reply(
            f"✅ {kind_name} 本次结束，用时 {used_min_round} 分钟（上限 {limit_min} 分钟）。\n"
            f"{tip}\n"
            f"今日 {kind_name}：第 {used_times} 次（上限 {max_times} 次），剩余 {remain} 次。"
        )
        return

    # 2) 上班：记录上班打卡（仅打卡上班，不做下班）
    if is_checkin(text):
        await add_checkin(chat_id, user_id, username, full_name, dt)
        await msg.reply(nice_checkin_reply())
        return

    # 3) 外出：开始外出
    kind = match_break_kind(text)
    if kind:
        # 如果已经在外出中，提示
        active = await get_active_session(chat_id, user_id)
        if active:
            kind0 = active["kind"]
            nm0 = BREAK_RULES.get(kind0, {}).get("name", kind0)
            st0 = active["start_at"].astimezone(TZ).strftime("%H:%M")
            await msg.reply(f"⚠️ 你已经在外出：{nm0}（开始于 {st0}）。回来请发：回/back/1/结束")
            return

        limit_min = BREAK_RULES[kind]["limit_min"]
        max_times = BREAK_RULES[kind]["max_times"]
        wd = dt.date()
        used_times_before = await count_break_used(chat_id, user_id, wd, kind)
        used_times_after = used_times_before + 1
        remain_after = max(0, max_times - used_times_after)

        session_id = await start_break(chat_id, user_id, username, full_name, kind, dt)
        kind_name = BREAK_RULES[kind]["name"]

        # 外出开始：发两条消息（回来时删除）
        msg1 = await msg.reply(
            f"✅ 已记录：{kind_name}\n"
            f"今日第 {used_times_after} 次（上限 {max_times} 次），剩余 {remain_after} 次。\n"
            f"请在 {deadline_text(dt, limit_min)} 前回来（上限 {limit_min} 分钟）。"
        )
        msg2 = await msg.reply("🔁 回来请发：回 / back / 1 / 结束")
        await set_break_msgs(session_id, msg1.message_id, msg2.message_id)
        return

    # 其他文本：不处理（避免刷屏）
    return


async def main():
    await db_init()
    # ✅ Railway 上经常残留 webhook，这行必须放在 async 函数里
    await bot.delete_webhook(drop_pending_updates=True)
    print("[bot] polling started")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
