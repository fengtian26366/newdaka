import os
import re
import asyncio
from io import BytesIO
from datetime import datetime, timedelta, date, time
from zoneinfo import ZoneInfo

import asyncpg
from dotenv import load_dotenv
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, BufferedInputFile
from aiogram.filters import Command, CommandStart

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "").strip()
ADMIN_IDS = {int(x.strip()) for x in ADMIN_IDS_RAW.split(",") if x.strip().isdigit()} if ADMIN_IDS_RAW else set()

if not BOT_TOKEN or not DATABASE_URL:
    raise RuntimeError("Missing BOT_TOKEN or DATABASE_URL")

TZ = ZoneInfo("Asia/Colombo")

# ====== 写死规则（你确认的）======
SHIFT_DAY_START = time(7, 0)
SHIFT_DAY_END = time(19, 0)   # 19:00 切到夜班（>=19:00 是夜班）

BREAK_RULES = {
    "PEE":   {"minutes": 6,  "max_count": 3, "aliases": ["小便", "尿", "pee"]},
    "POOP":  {"minutes": 15, "max_count": 2, "aliases": ["大便", "拉屎", "poop"]},
    "EAT":   {"minutes": 30, "max_count": 3, "aliases": ["吃饭", "eat", "meal", "饭"]},
    "SMOKE": {"minutes": 10, "max_count": 5, "aliases": ["抽烟", "抽", "smoke"]},
}

WORKIN_ALIASES = ["上班", "开工", "in", "start", "work in"]
TOTAL_BREAK_LIMIT_MIN = 188

def is_admin(user_id: int) -> bool:
    return (not ADMIN_IDS) or (user_id in ADMIN_IDS)

def norm_text(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^\w\u4e00-\u9fff\s]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def contains_any(text: str, aliases: list[str]) -> bool:
    for a in aliases:
        if a and a.lower() in text:
            return True
    return False

def get_shift(local_dt: datetime) -> tuple[date, str]:
    """
    返回：(shift_date, shift_type)
    - DAY: 07:00-18:59，shift_date = 当天日期
    - NIGHT: 19:00-06:59
        - 若时间在 19:00-23:59：shift_date = 当天日期
        - 若时间在 00:00-06:59：shift_date = 前一天日期（夜班属于前一天19点那班）
    """
    t = local_dt.time()
    d = local_dt.date()

    if SHIFT_DAY_START <= t < SHIFT_DAY_END:
        return d, "DAY"
    # NIGHT
    if t < SHIFT_DAY_START:
        return (d - timedelta(days=1)), "NIGHT"
    return d, "NIGHT"

def local_str(dt_utc: datetime | None) -> str:
    if not dt_utc:
        return ""
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=ZoneInfo("UTC"))
    return dt_utc.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S")

DDL = """
CREATE TABLE IF NOT EXISTS tg_groups (
  chat_id BIGINT PRIMARY KEY,
  title TEXT,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS known_users (
  chat_id BIGINT NOT NULL,
  user_id BIGINT NOT NULL,
  user_name TEXT,
  first_seen TIMESTAMP NOT NULL DEFAULT NOW(),
  last_seen TIMESTAMP NOT NULL DEFAULT NOW(),
  PRIMARY KEY(chat_id, user_id)
);

CREATE TABLE IF NOT EXISTS shift_attendance (
  chat_id BIGINT NOT NULL,
  shift_date DATE NOT NULL,
  shift_type TEXT NOT NULL, -- DAY / NIGHT
  user_id BIGINT NOT NULL,
  user_name TEXT,
  first_in_at TIMESTAMP NULL,      -- 记录第一次“上班”
  last_action_at TIMESTAMP NULL,

  pee_count INT NOT NULL DEFAULT 0,
  poop_count INT NOT NULL DEFAULT 0,
  eat_count INT NOT NULL DEFAULT 0,
  smoke_count INT NOT NULL DEFAULT 0,

  pee_min INT NOT NULL DEFAULT 0,
  poop_min INT NOT NULL DEFAULT 0,
  eat_min INT NOT NULL DEFAULT 0,
  smoke_min INT NOT NULL DEFAULT 0,

  violation BOOLEAN NOT NULL DEFAULT FALSE,
  violation_reason TEXT NOT NULL DEFAULT '',

  PRIMARY KEY(chat_id, shift_date, shift_type, user_id)
);

CREATE INDEX IF NOT EXISTS idx_shift_date ON shift_attendance(chat_id, shift_date);
"""

pool: asyncpg.Pool | None = None

async def db_init():
    global pool
    last_err = None
    for i in range(30):
        try:
            pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
            async with pool.acquire() as conn:
                await conn.execute("SELECT 1;")
                await conn.execute(DDL)
            print("[db] connected & migrated")
            return
        except Exception as e:
            last_err = e
            print(f"[db] connect failed ({i+1}/30): {e}")
            await asyncio.sleep(2)
    raise RuntimeError(f"DB connect failed after retries: {last_err}")

async def upsert_group(chat_id: int, title: str):
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO tg_groups(chat_id, title) VALUES($1,$2) "
            "ON CONFLICT(chat_id) DO UPDATE SET title=EXCLUDED.title",
            chat_id, title
        )

async def upsert_known_user(chat_id: int, user_id: int, user_name: str):
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO known_users(chat_id, user_id, user_name) VALUES($1,$2,$3) "
            "ON CONFLICT(chat_id, user_id) DO UPDATE SET user_name=EXCLUDED.user_name, last_seen=NOW()",
            chat_id, user_id, user_name
        )

async def get_or_create_shift_row(chat_id: int, shift_date: date, shift_type: str, user_id: int, user_name: str):
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO shift_attendance(chat_id, shift_date, shift_type, user_id, user_name) "
            "VALUES($1,$2,$3,$4,$5) "
            "ON CONFLICT(chat_id, shift_date, shift_type, user_id) DO UPDATE SET user_name=EXCLUDED.user_name",
            chat_id, shift_date, shift_type, user_id, user_name
        )

async def update_workin(chat_id: int, shift_date: date, shift_type: str, user_id: int):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE shift_attendance
            SET first_in_at = COALESCE(first_in_at, NOW()),
                last_action_at = NOW()
            WHERE chat_id=$1 AND shift_date=$2 AND shift_type=$3 AND user_id=$4
            """,
            chat_id, shift_date, shift_type, user_id
        )

def compute_violation(counts: dict, mins: dict) -> tuple[bool, str]:
    reasons = []
    total_min = sum(mins.values())
    if total_min > TOTAL_BREAK_LIMIT_MIN:
        reasons.append(f"总离岗{total_min}>{TOTAL_BREAK_LIMIT_MIN}")

    for k, rule in BREAK_RULES.items():
        if counts.get(k, 0) > rule["max_count"]:
            reasons.append(f"{k}次数{counts[k]}>{rule['max_count']}")
    return (len(reasons) > 0, "; ".join(reasons))

async def update_break(chat_id: int, shift_date: date, shift_type: str, user_id: int, kind: str):
    rule = BREAK_RULES[kind]
    async with pool.acquire() as conn:
        # 先加计数与分钟
        await conn.execute(
            f"""
            UPDATE shift_attendance
            SET {kind.lower()}_count = {kind.lower()}_count + 1,
                {kind.lower()}_min   = {kind.lower()}_min + $5,
                last_action_at = NOW()
            WHERE chat_id=$1 AND shift_date=$2 AND shift_type=$3 AND user_id=$4
            """,
            chat_id, shift_date, shift_type, user_id, rule["minutes"]
        )

        # 取当前统计计算违规
        row = await conn.fetchrow(
            """
            SELECT pee_count, poop_count, eat_count, smoke_count,
                   pee_min, poop_min, eat_min, smoke_min
            FROM shift_attendance
            WHERE chat_id=$1 AND shift_date=$2 AND shift_type=$3 AND user_id=$4
            """,
            chat_id, shift_date, shift_type, user_id
        )
        counts = {"PEE": row["pee_count"], "POOP": row["poop_count"], "EAT": row["eat_count"], "SMOKE": row["smoke_count"]}
        mins = {"PEE": row["pee_min"], "POOP": row["poop_min"], "EAT": row["eat_min"], "SMOKE": row["smoke_min"]}
        viol, reason = compute_violation(counts, mins)

        await conn.execute(
            """
            UPDATE shift_attendance
            SET violation=$5, violation_reason=$6
            WHERE chat_id=$1 AND shift_date=$2 AND shift_type=$3 AND user_id=$4
            """,
            chat_id, shift_date, shift_type, user_id, viol, reason
        )

async def export_xlsx(chat_id: int, start_d: date, end_d: date) -> bytes:
    # end_d inclusive
    async with pool.acquire() as conn:
        users = await conn.fetch(
            "SELECT user_id, user_name FROM known_users WHERE chat_id=$1 ORDER BY user_name NULLS LAST, user_id",
            chat_id
        )

        # 构造所有班次 key
        shifts = []
        d = start_d
        while d <= end_d:
            shifts.append((d, "DAY"))
            shifts.append((d, "NIGHT"))
            d += timedelta(days=1)

        # 把已有记录一次性拉出来
        rows = await conn.fetch(
            """
            SELECT shift_date, shift_type, user_id, user_name, first_in_at,
                   pee_count, poop_count, eat_count, smoke_count,
                   pee_min, poop_min, eat_min, smoke_min,
                   violation, violation_reason
            FROM shift_attendance
            WHERE chat_id=$1 AND shift_date >= $2 AND shift_date <= $3
            """,
            chat_id, start_d, end_d
        )

    data_map = {}
    for r in rows:
        key = (r["shift_date"], r["shift_type"], int(r["user_id"]))
        data_map[key] = r

    wb = Workbook()

    # Sheet 1: summary
    ws = wb.active
    ws.title = "summary"
    headers = [
        "shift_date", "shift_type", "user_id", "user_name",
        "clock_in_time(Colombo)", "status",
        "break_total_min",
        "eat_count", "smoke_count", "pee_count", "poop_count",
        "eat_min", "smoke_min", "pee_min", "poop_min",
        "violation", "violation_reason"
    ]
    ws.append(headers)
    bold = Font(bold=True)
    for c in ws[1]:
        c.font = bold

    red_fill = PatternFill("solid", fgColor="FFCCCC")
    yellow_fill = PatternFill("solid", fgColor="FFF2CC")

    for (sd, st) in shifts:
        for u in users:
            uid = int(u["user_id"])
            uname = u["user_name"] or str(uid)
            r = data_map.get((sd, st, uid))

            if r and r["first_in_at"]:
                status = "OK"
                cin = local_str(r["first_in_at"])
            else:
                status = "MISSING"
                cin = ""

            if r:
                eat_c, smk_c, pee_c, pop_c = r["eat_count"], r["smoke_count"], r["pee_count"], r["poop_count"]
                eat_m, smk_m, pee_m, pop_m = r["eat_min"], r["smoke_min"], r["pee_min"], r["poop_min"]
                viol, reason = r["violation"], r["violation_reason"]
            else:
                eat_c = smk_c = pee_c = pop_c = 0
                eat_m = smk_m = pee_m = pop_m = 0
                viol, reason = False, ""

            total_break = eat_m + smk_m + pee_m + pop_m

            ws.append([
                sd.strftime("%Y-%m-%d"), st, uid, uname,
                cin, status,
                total_break,
                eat_c, smk_c, pee_c, pop_c,
                eat_m, smk_m, pee_m, pop_m,
                "YES" if viol else "NO",
                reason
            ])

            row_idx = ws.max_row
            if status == "MISSING":
                for cell in ws[row_idx]:
                    cell.fill = red_fill
            elif viol:
                for cell in ws[row_idx]:
                    cell.fill = yellow_fill

    # Sheet 2: rules
    ws2 = wb.create_sheet("rules")
    ws2.append(["timezone", "Asia/Colombo"])
    ws2.append(["shift DAY", "07:00-18:59"])
    ws2.append(["shift NIGHT", "19:00-06:59 (belongs to shift_date of 19:00 day)"])
    ws2.append(["TOTAL_BREAK_LIMIT_MIN", TOTAL_BREAK_LIMIT_MIN])
    ws2.append([])
    ws2.append(["TYPE", "minutes_each", "max_count", "aliases"])
    for k, v in BREAK_RULES.items():
        ws2.append([k, v["minutes"], v["max_count"], ", ".join(v["aliases"])])

    # autosize (简单处理)
    for sheet in (ws, ws2):
        for col in sheet.columns:
            max_len = 0
            col_letter = col[0].column_letter
            for cell in col:
                val = str(cell.value) if cell.value is not None else ""
                max_len = max(max_len, len(val))
            sheet.column_dimensions[col_letter].width = min(max_len + 2, 40)

    bio = BytesIO()
    wb.save(bio)
    return bio.getvalue()

# ====== Bot ======
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

@dp.message(CommandStart())
async def start(m: Message):
    if m.chat.type == "private":
        if not is_admin(m.from_user.id):
            await m.reply("无权限。")
            return
        await m.reply(
            "✅ 打卡机器人已启动（写死规则版）\n\n"
            "群里直接发：上班/in、吃饭/eat、小便/pee、大便/poop、抽烟/smoke\n"
            "（不打下班，按斯里兰卡时间自动分白班/夜班）\n\n"
            "私聊导出：\n"
            "/export <chat_id> 2026-02-01 2026-02-05\n"
            "示例：/export -1001234567890 2026-02-01 2026-02-05\n\n"
            "如何拿 chat_id：把机器人拉进群，在群里随便发一句，机器人会记录群；你也可以让我后续加 /mygroups 自动列群。"
        )
    else:
        await upsert_group(m.chat.id, m.chat.title or "")
        await m.reply("✅ 已加入群。直接发 上班/吃饭/eat/抽烟/小便/大便 即可记录。")

@dp.message(F.chat.type.in_(["group", "supergroup"]) & F.text)
async def group_listener(m: Message):
    await upsert_group(m.chat.id, m.chat.title or "")

    text = norm_text(m.text)
    if not text:
        return

    uid = m.from_user.id
    uname = m.from_user.full_name or (m.from_user.username or str(uid))
    await upsert_known_user(m.chat.id, uid, uname)

    now_local = datetime.now(tz=TZ)
    shift_date, shift_type = get_shift(now_local)

    await get_or_create_shift_row(m.chat.id, shift_date, shift_type, uid, uname)

    # 上班
    if contains_any(text, WORKIN_ALIASES):
        await update_workin(m.chat.id, shift_date, shift_type, uid)
        return

    # 离岗类型
    for kind, rule in BREAK_RULES.items():
        if contains_any(text, rule["aliases"]):
            await update_break(m.chat.id, shift_date, shift_type, uid, kind)
            return

@dp.message(Command("export"))
async def export_cmd(m: Message):
    if m.chat.type != "private":
        return
    if not is_admin(m.from_user.id):
        await m.reply("无权限。")
        return

    parts = m.text.split()
    if len(parts) not in (2, 4):
        await m.reply("用法：/export <chat_id> 或 /export <chat_id> YYYY-MM-DD YYYY-MM-DD")
        return

    try:
        chat_id = int(parts[1])
    except:
        await m.reply("chat_id 格式不对，例如 -1001234567890")
        return

    if len(parts) == 2:
        start_d = date.today()
        end_d = start_d
    else:
        try:
            start_d = datetime.strptime(parts[2], "%Y-%m-%d").date()
            end_d = datetime.strptime(parts[3], "%Y-%m-%d").date()
        except:
            await m.reply("日期格式不对：YYYY-MM-DD")
            return

    data = await export_xlsx(chat_id, start_d, end_d)
    fn = f"attendance_{chat_id}_{start_d.strftime('%Y%m%d')}_{end_d.strftime('%Y%m%d')}.xlsx"
    await m.reply_document(BufferedInputFile(data, filename=fn), caption="✅ 导出完成（红色=MISSING，黄色=违规）")

async def main():
    await db_init()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
