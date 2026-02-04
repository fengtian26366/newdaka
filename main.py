import os
import re
import asyncio
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from io import BytesIO
from typing import Optional, Dict, List, Tuple

from dotenv import load_dotenv
import asyncpg
from openpyxl import Workbook
from openpyxl.utils import get_column_letter

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, CallbackQuery, BufferedInputFile
from aiogram.filters import Command

# ======================
# 固定配置（写死）
# ======================
TZ = ZoneInfo("Asia/Colombo")

SHIFT_START_HOUR = 7
SHIFT_END_HOUR = 19

# 每种类型：上限次数、单次允许分钟
BREAK_RULES = {
    "pee":  {"name": "小便", "max_times": 3, "limit_min": 6,  "keywords": ["小便", "尿", "pee", "p"]},
    "poo":  {"name": "大便", "max_times": 2, "limit_min": 15, "keywords": ["大便", "拉屎", "poo", "shit"]},
    "meal": {"name": "吃饭", "max_times": 3, "limit_min": 30, "keywords": ["吃饭", "eat", "meal"]},
    "smk":  {"name": "抽烟", "max_times": 5, "limit_min": 10, "keywords": ["抽烟", "抽", "smoke", "smk"]},
}

CHECKIN_KEYWORDS = ["上班", "开工", "上工", "in", "start", "checkin"]
BACK_KEYWORDS = ["回", "回来", "back", "1", "结束", "done", "return"]

PRAISE_LINES = [
    "✅ 已打卡上班，辛苦啦！今天也要稳稳的～",
    "✅ 上班打卡成功！加油，今天一定顺利。",
    "✅ 已记录上班！注意节奏，别太累。",
    "✅ 打卡成功，辛苦辛苦～",
]

# ======================
# 环境变量
# ======================
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").replace(" ", "").split(",") if x.strip().isdigit()]

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN 未设置")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL 未设置")


# ======================
# DB
# ======================
pool: asyncpg.Pool = None

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS groups(
  chat_id BIGINT PRIMARY KEY,
  title TEXT,
  added_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS users(
  user_id BIGINT PRIMARY KEY,
  first_name TEXT,
  username TEXT
);

-- 记录在某群出现过（用于导出“名单”）
CREATE TABLE IF NOT EXISTS group_members(
  chat_id BIGINT NOT NULL,
  user_id BIGINT NOT NULL,
  last_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY(chat_id, user_id)
);

CREATE TABLE IF NOT EXISTS attendance(
  chat_id BIGINT NOT NULL,
  user_id BIGINT NOT NULL,
  day DATE NOT NULL,
  checkin_at TIMESTAMPTZ,
  PRIMARY KEY(chat_id, user_id, day)
);

CREATE TABLE IF NOT EXISTS break_sessions(
  id BIGSERIAL PRIMARY KEY,
  chat_id BIGINT NOT NULL,
  user_id BIGINT NOT NULL,
  day DATE NOT NULL,
  kind TEXT NOT NULL,          -- pee/poo/meal/smk
  start_at TIMESTAMPTZ NOT NULL,
  end_at TIMESTAMPTZ,
  duration_sec INT,
  exceeded BOOLEAN NOT NULL DEFAULT FALSE
);

-- 当前进行中的“离开”提示消息（用于回来时删消息并总结）
CREATE TABLE IF NOT EXISTS active_notifs(
  chat_id BIGINT NOT NULL,
  user_id BIGINT NOT NULL,
  session_id BIGINT NOT NULL,
  bot_msg_id BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY(chat_id, user_id)
);
"""

async def db_init():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    async with pool.acquire() as conn:
        await conn.execute(CREATE_SQL)


# ======================
# 工具函数
# ======================
def now_tz() -> datetime:
    return datetime.now(TZ)

def today_tz() -> date:
    return now_tz().date()

def norm_text(t: str) -> str:
    return (t or "").strip().lower()

def match_break_kind(text: str) -> Optional[str]:
    t = norm_text(text)
    for kind, rule in BREAK_RULES.items():
        for kw in rule["keywords"]:
            if t == kw.lower():
                return kind
    return None

def is_checkin(text: str) -> bool:
    t = norm_text(text)
    return any(t == kw.lower() for kw in CHECKIN_KEYWORDS)

def is_back(text: str) -> bool:
    t = norm_text(text)
    return any(t == kw.lower() for kw in BACK_KEYWORDS)

def fmt_hhmm(dt: datetime) -> str:
    return dt.astimezone(TZ).strftime("%H:%M")

def rand_praise(seed: int) -> str:
    return PRAISE_LINES[seed % len(PRAISE_LINES)]

async def upsert_group_user(chat_id: int, chat_title: str, user_id: int, first_name: str, username: str):
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO groups(chat_id,title) VALUES($1,$2) "
            "ON CONFLICT(chat_id) DO UPDATE SET title=EXCLUDED.title",
            chat_id, chat_title or ""
        )
        await conn.execute(
            "INSERT INTO users(user_id,first_name,username) VALUES($1,$2,$3) "
            "ON CONFLICT(user_id) DO UPDATE SET first_name=EXCLUDED.first_name, username=EXCLUDED.username",
            user_id, first_name or "", username or ""
        )
        await conn.execute(
            "INSERT INTO group_members(chat_id,user_id,last_seen) VALUES($1,$2,now()) "
            "ON CONFLICT(chat_id,user_id) DO UPDATE SET last_seen=now()",
            chat_id, user_id
        )

async def get_or_create_attendance(chat_id: int, user_id: int, day: date):
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO attendance(chat_id,user_id,day) VALUES($1,$2,$3) ON CONFLICT DO NOTHING",
            chat_id, user_id, day
        )

async def set_checkin(chat_id: int, user_id: int, day: date, ts: datetime) -> bool:
    """返回 True 表示这次是首次打卡；False 表示已经打过卡"""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT checkin_at FROM attendance WHERE chat_id=$1 AND user_id=$2 AND day=$3",
            chat_id, user_id, day
        )
        if row and row["checkin_at"]:
            return False
        await conn.execute(
            "INSERT INTO attendance(chat_id,user_id,day,checkin_at) VALUES($1,$2,$3,$4) "
            "ON CONFLICT(chat_id,user_id,day) DO UPDATE SET checkin_at=EXCLUDED.checkin_at",
            chat_id, user_id, day, ts
        )
        return True

async def get_active_session(chat_id: int, user_id: int) -> Optional[asyncpg.Record]:
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM break_sessions WHERE chat_id=$1 AND user_id=$2 AND end_at IS NULL ORDER BY start_at DESC LIMIT 1",
            chat_id, user_id
        )

async def count_today(chat_id: int, user_id: int, day: date, kind: str) -> int:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT COUNT(*) AS c FROM break_sessions WHERE chat_id=$1 AND user_id=$2 AND day=$3 AND kind=$4",
            chat_id, user_id, day, kind
        )
        return int(row["c"] or 0)

async def start_break(chat_id: int, user_id: int, day: date, kind: str, start_at: datetime) -> int:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO break_sessions(chat_id,user_id,day,kind,start_at) VALUES($1,$2,$3,$4,$5) RETURNING id",
            chat_id, user_id, day, kind, start_at
        )
        return int(row["id"])

async def finish_break(session_id: int, end_at: datetime, exceeded: bool) -> int:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT start_at FROM break_sessions WHERE id=$1",
            session_id
        )
        if not row:
            return 0
        start_at = row["start_at"]
        dur = int((end_at - start_at).total_seconds())
        await conn.execute(
            "UPDATE break_sessions SET end_at=$1, duration_sec=$2, exceeded=$3 WHERE id=$4",
            end_at, dur, exceeded, session_id
        )
        return dur

async def set_active_notif(chat_id: int, user_id: int, session_id: int, bot_msg_id: int):
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO active_notifs(chat_id,user_id,session_id,bot_msg_id) VALUES($1,$2,$3,$4) "
            "ON CONFLICT(chat_id,user_id) DO UPDATE SET session_id=EXCLUDED.session_id, bot_msg_id=EXCLUDED.bot_msg_id, created_at=now()",
            chat_id, user_id, session_id, bot_msg_id
        )

async def pop_active_notif(chat_id: int, user_id: int) -> Optional[asyncpg.Record]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM active_notifs WHERE chat_id=$1 AND user_id=$2",
            chat_id, user_id
        )
        await conn.execute("DELETE FROM active_notifs WHERE chat_id=$1 AND user_id=$2", chat_id, user_id)
        return row

async def list_groups() -> List[asyncpg.Record]:
    async with pool.acquire() as conn:
        return await conn.fetch("SELECT chat_id,title FROM groups ORDER BY added_at DESC")

async def fetch_export(chat_id: int, d1: date, d2: date) -> Tuple[List[asyncpg.Record], List[asyncpg.Record], List[asyncpg.Record]]:
    """
    返回：members, attendance, breaks
    members: group_members join users
    attendance: attendance rows
    breaks: break_sessions rows
    """
    async with pool.acquire() as conn:
        members = await conn.fetch(
            "SELECT gm.user_id,u.first_name,u.username FROM group_members gm "
            "LEFT JOIN users u ON u.user_id=gm.user_id "
            "WHERE gm.chat_id=$1 ORDER BY gm.user_id",
            chat_id
        )
        attendance = await conn.fetch(
            "SELECT * FROM attendance WHERE chat_id=$1 AND day BETWEEN $2 AND $3",
            chat_id, d1, d2
        )
        breaks = await conn.fetch(
            "SELECT * FROM break_sessions WHERE chat_id=$1 AND day BETWEEN $2 AND $3",
            chat_id, d1, d2
        )
        return members, attendance, breaks

def build_xlsx(chat_title: str, d1: date, d2: date,
               members: List[asyncpg.Record],
               attendance: List[asyncpg.Record],
               breaks: List[asyncpg.Record]) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = "打卡统计"

    # index maps
    att_map = {(r["user_id"], r["day"]): r["checkin_at"] for r in attendance}
    # breaks aggregate: (user, day, kind) -> list(durations), exceeded_any
    agg: Dict[Tuple[int, date, str], Dict[str, object]] = {}
    for b in breaks:
        key = (b["user_id"], b["day"], b["kind"])
        if key not in agg:
            agg[key] = {"durations": [], "exceeded": False}
        if b["duration_sec"] is not None:
            agg[key]["durations"].append(int(b["duration_sec"]))
        if b["exceeded"]:
            agg[key]["exceeded"] = True

    # header
    headers = ["日期", "用户ID", "姓名", "用户名", "上班打卡", "备注"]
    for kind, rule in BREAK_RULES.items():
        headers += [f"{rule['name']}次数", f"{rule['name']}总时长(分)", f"{rule['name']}超时?"]
    ws.append(headers)

    # date range
    cur = d1
    while cur <= d2:
        for m in members:
            uid = int(m["user_id"])
            name = m["first_name"] or ""
            uname = m["username"] or ""
            checkin_at = att_map.get((uid, cur))
            checkin_str = ""
            remark = ""
            if checkin_at:
                checkin_str = checkin_at.astimezone(TZ).strftime("%H:%M:%S")
            else:
                remark = "未打卡上班"

            row = [cur.isoformat(), uid, name, uname, checkin_str, remark]

            for kind, rule in BREAK_RULES.items():
                info = agg.get((uid, cur, kind), {"durations": [], "exceeded": False})
                times = len(info["durations"])
                total_min = round(sum(info["durations"]) / 60.0, 2) if info["durations"] else 0
                exceeded = "是" if info["exceeded"] else ""
                row += [times, total_min, exceeded]

            ws.append(row)
        cur += timedelta(days=1)

    # beautify columns
    for i, _ in enumerate(headers, start=1):
        ws.column_dimensions[get_column_letter(i)].width = 16

    # sheet2: 明细
    ws2 = wb.create_sheet("离开明细")
    ws2.append(["日期", "用户ID", "类型", "开始", "结束", "用时(分)", "超时?"])
    for b in breaks:
        s = b["start_at"].astimezone(TZ).strftime("%H:%M:%S")
        e = b["end_at"].astimezone(TZ).strftime("%H:%M:%S") if b["end_at"] else ""
        durm = round((b["duration_sec"] or 0) / 60.0, 2) if b["duration_sec"] else ""
        ws2.append([b["day"].isoformat(), int(b["user_id"]), b["kind"], s, e, durm, "是" if b["exceeded"] else ""])
    for i in range(1, 8):
        ws2.column_dimensions[get_column_letter(i)].width = 18

    bio = BytesIO()
    wb.save(bio)
    return bio.getvalue()


# ======================
# Bot & Router
# ======================
bot = Bot(BOT_TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)


# -------- 群里：普通文本打卡 --------
@router.message(F.chat.type.in_({"group", "supergroup"}) & F.text)
async def on_group_text(msg: Message):
    chat_id = msg.chat.id
    user = msg.from_user
    if not user:
        return

    text = msg.text or ""
    t = norm_text(text)
    now = now_tz()
    day = now.date()

    # 记录群、用户出现过
    await upsert_group_user(chat_id, msg.chat.title or "", user.id, user.first_name or "", user.username or "")
    await get_or_create_attendance(chat_id, user.id, day)

    # 1) 回来 / 结束
    if is_back(t):
        active = await get_active_session(chat_id, user.id)
        if not active:
            await msg.reply("⚠️ 你当前没有进行中的离开记录。")
            return

        kind = active["kind"]
        rule = BREAK_RULES.get(kind)
        limit_min = int(rule["limit_min"])
        due = active["start_at"].astimezone(TZ) + timedelta(minutes=limit_min)
        exceeded = now_tz() > due

        # 结算
        dur_sec = await finish_break(int(active["id"]), now_tz(), exceeded)

        # 删除之前的提示消息（只能删机器人自己发的；需要权限才删得掉）
        notif = await pop_active_notif(chat_id, user.id)
        if notif:
            try:
                await bot.delete_message(chat_id, int(notif["bot_msg_id"]))
            except Exception:
                pass  # 没权限/已被删都无所谓

        # 统计剩余次数
        used_times = await count_today(chat_id, user.id, day, kind)
        remaining = max(0, int(rule["max_times"]) - used_times)

        dur_min = round(dur_sec / 60.0, 2)
        extra = "⚠️ 本次已超时。" if exceeded else "✅ 本次未超时。"

        await msg.reply(
            f"✅ 已回来，{rule['name']}本次结束：用时 {dur_min} 分钟。\n"
            f"{extra}\n"
            f"今日 {rule['name']}：已用 {used_times}/{rule['max_times']} 次，剩余 {remaining} 次。"
        )
        return

    # 2) 上班打卡
    if is_checkin(t):
        first = await set_checkin(chat_id, user.id, day, now)
        if first:
            await msg.reply(rand_praise(user.id))
        else:
            await msg.reply("ℹ️ 今天已经打过上班卡了（已记录）。")
        return

    # 3) 离开类型（吃饭/抽烟/小便/大便）
    kind = match_break_kind(t)
    if kind:
        # 如果正在进行中，禁止再开新的
        active = await get_active_session(chat_id, user.id)
        if active:
            rule = BREAK_RULES.get(active["kind"])
            limit_min = int(rule["limit_min"])
            due = active["start_at"].astimezone(TZ) + timedelta(minutes=limit_min)
            await msg.reply(f"⚠️ 你正在 {rule['name']} 中，请先发 回/back/1 结束。最晚 {fmt_hhmm(due)} 前回来。")
            return

        rule = BREAK_RULES[kind]
        used_times_before = await count_today(chat_id, user.id, day, kind)
        next_times = used_times_before + 1

        # 允许继续记录，但提示超出次数
        warn = ""
        if next_times > int(rule["max_times"]):
            warn = f"\n⚠️ 注意：你这次已经超过 {rule['name']} 上限次数（上限 {rule['max_times']}）。"

        session_id = await start_break(chat_id, user.id, day, kind, now)
        due = now + timedelta(minutes=int(rule["limit_min"]))
        remaining = max(0, int(rule["max_times"]) - next_times)

        bot_reply = await msg.reply(
            f"🕒 已记录：{rule['name']}（第 {next_times}/{rule['max_times']} 次）。\n"
            f"请在 {fmt_hhmm(due)} 前回来，回来发：回/back/1/结束。\n"
            f"剩余次数：{remaining} 次。{warn}"
        )
        # 保存提示消息ID，用于回来时删除
        await set_active_notif(chat_id, user.id, session_id, bot_reply.message_id)
        return

    # 4) 其它文字不处理（避免刷屏）
    return


# -------- 私聊：/start /mygroups /export --------
@router.message(F.chat.type == "private", Command("start"))
async def on_private_start(msg: Message):
    await msg.reply(
        "✅ 兰卡打卡机器人已启动。\n\n"
        "【群里直接发】\n"
        "上班/开工/in  → 记录上班打卡\n"
        "吃饭/eat、抽烟/抽、小便/尿、大便/拉屎 → 记录离开（会提示最晚回来时间）\n"
        "回/back/1/结束 → 结束离开并统计用时\n\n"
        "【私聊命令】\n"
        "/mygroups  查看机器人加入的群\n"
        "/export 2026-02-01 2026-02-05  导出指定日期范围 XLSX（会让你选择群）\n"
    )


@router.message(F.chat.type == "private", Command("mygroups"))
async def on_mygroups(msg: Message):
    groups = await list_groups()
    if not groups:
        await msg.reply("暂无群记录（把机器人拉进群并发一条消息试试）。")
        return
    lines = ["📌 机器人加入的群："]
    for g in groups[:50]:
        lines.append(f"- {g['title'] or '(无标题)'}  |  chat_id={g['chat_id']}")
    await msg.reply("\n".join(lines))


# export 格式：/export 2026-02-01 2026-02-05
EXPORT_RE = re.compile(r"^/export\s+(\d{4}-\d{2}-\d{2})\s+(\d{4}-\d{2}-\d{2})\s*$")

@router.message(F.chat.type == "private", F.text)
async def on_export(msg: Message):
    t = (msg.text or "").strip()
    m = EXPORT_RE.match(t)
    if not m:
        return

    d1 = date.fromisoformat(m.group(1))
    d2 = date.fromisoformat(m.group(2))
    if d2 < d1:
        await msg.reply("日期范围不对：结束日期不能早于开始日期。")
        return
    if (d2 - d1).days > 60:
        await msg.reply("一次最多导出 60 天，避免文件太大。")
        return

    groups = await list_groups()
    if not groups:
        await msg.reply("暂无群记录（先把机器人拉进群并发一条消息）。")
        return

    # 简单：如果只有一个群就直接导出；多个群让你输入 chat_id
    if len(groups) == 1:
        chat_id = int(groups[0]["chat_id"])
        chat_title = groups[0]["title"] or "群"
        await do_export(msg, chat_id, chat_title, d1, d2)
        return

    # 多群：提示你用 /export_chat <chat_id> d1 d2
    lines = ["你有多个群，请用下面格式导出：",
             f"/export_chat <chat_id> {d1.isoformat()} {d2.isoformat()}",
             "",
             "可选 chat_id："]
    for g in groups[:30]:
        lines.append(f"- {g['title'] or '(无标题)'}  |  {g['chat_id']}")
    await msg.reply("\n".join(lines))


EXPORT_CHAT_RE = re.compile(r"^/export_chat\s+(-?\d+)\s+(\d{4}-\d{2}-\d{2})\s+(\d{4}-\d{2}-\d{2})\s*$")

@router.message(F.chat.type == "private", F.text)
async def on_export_chat(msg: Message):
    t = (msg.text or "").strip()
    m = EXPORT_CHAT_RE.match(t)
    if not m:
        return
    chat_id = int(m.group(1))
    d1 = date.fromisoformat(m.group(2))
    d2 = date.fromisoformat(m.group(3))
    if d2 < d1:
        await msg.reply("日期范围不对：结束日期不能早于开始日期。")
        return
    if (d2 - d1).days > 60:
        await msg.reply("一次最多导出 60 天，避免文件太大。")
        return

    # 找标题
    groups = await list_groups()
    title = None
    for g in groups:
        if int(g["chat_id"]) == chat_id:
            title = g["title"] or "群"
            break
    if title is None:
        await msg.reply("这个 chat_id 没找到（先 /mygroups 看一下）。")
        return

    await do_export(msg, chat_id, title, d1, d2)


async def do_export(msg: Message, chat_id: int, chat_title: str, d1: date, d2: date):
    members, attendance, breaks = await fetch_export(chat_id, d1, d2)
    if not members:
        await msg.reply("这个群还没有成员记录（至少要有人在群里发过消息）。")
        return

    xlsx_bytes = build_xlsx(chat_title, d1, d2, members, attendance, breaks)
    filename = f"打卡统计_{chat_title}_{d1.isoformat()}_{d2.isoformat()}.xlsx".replace("/", "_")

    await msg.reply_document(
        BufferedInputFile(xlsx_bytes, filename=filename),
        caption=f"✅ 导出完成：{chat_title}\n日期：{d1.isoformat()} ~ {d2.isoformat()}"
    )


# ======================
# main
# ======================
async def main():
    await db_init()

    # ✅ 关键：强制切回 polling，避免“群里普通消息不进来”
    await bot.delete_webhook(drop_pending_updates=True)

    print("[bot] polling started")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
