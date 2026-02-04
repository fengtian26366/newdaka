import os
import re
import asyncio
from io import BytesIO
from datetime import datetime, timedelta, date
from typing import List, Optional, Tuple

import asyncpg
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    BufferedInputFile
)
from aiogram.filters import Command, CommandStart
from dotenv import load_dotenv
from openpyxl import Workbook

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
# 可选：只允许这些管理员私聊配置（逗号分隔）
ADMIN_IDS = set()
_admin_raw = os.getenv("ADMIN_IDS", "").strip()
if _admin_raw:
    for x in _admin_raw.split(","):
        x = x.strip()
        if x.isdigit():
            ADMIN_IDS.add(int(x))

if not BOT_TOKEN or not DATABASE_URL:
    raise RuntimeError("缺少 BOT_TOKEN 或 DATABASE_URL 环境变量")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

pool: asyncpg.Pool = None

# ---------- DB ----------
DDL = """
CREATE TABLE IF NOT EXISTS tg_groups (
    chat_id BIGINT PRIMARY KEY,
    title TEXT,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- 上班/下班关键词（每群一份）
CREATE TABLE IF NOT EXISTS group_keywords (
    chat_id BIGINT PRIMARY KEY REFERENCES tg_groups(chat_id) ON DELETE CASCADE,
    work_in_keywords TEXT NOT NULL DEFAULT '上班,开工,in,start,work in',
    work_out_keywords TEXT NOT NULL DEFAULT '下班,off,end,work out,收工'
);

-- 每群最多5个类型
CREATE TABLE IF NOT EXISTS group_types (
    id SERIAL PRIMARY KEY,
    chat_id BIGINT NOT NULL REFERENCES tg_groups(chat_id) ON DELETE CASCADE,
    type_name TEXT NOT NULL,
    UNIQUE(chat_id, type_name)
);

CREATE TABLE IF NOT EXISTS type_keywords (
    id SERIAL PRIMARY KEY,
    type_id INT NOT NULL REFERENCES group_types(id) ON DELETE CASCADE,
    keyword TEXT NOT NULL,
    UNIQUE(type_id, keyword)
);

-- 打卡记录
CREATE TABLE IF NOT EXISTS attendance_logs (
    id BIGSERIAL PRIMARY KEY,
    chat_id BIGINT NOT NULL REFERENCES tg_groups(chat_id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL,
    user_name TEXT,
    action_type TEXT NOT NULL, -- WORK_IN / WORK_OUT / BREAK:<type_name>
    raw_text TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_logs_chat_time ON attendance_logs(chat_id, created_at);
"""

async def db_init():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    async with pool.acquire() as conn:
        await conn.execute(DDL)

async def upsert_group(chat_id: int, title: str):
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO tg_groups(chat_id, title) VALUES($1,$2) "
            "ON CONFLICT(chat_id) DO UPDATE SET title=EXCLUDED.title",
            chat_id, title
        )
        await conn.execute(
            "INSERT INTO group_keywords(chat_id) VALUES($1) ON CONFLICT DO NOTHING",
            chat_id
        )

async def is_group_enabled(chat_id: int) -> bool:
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT enabled FROM tg_groups WHERE chat_id=$1", chat_id)
        return bool(row["enabled"]) if row else True

def split_keywords(s: str) -> List[str]:
    # 支持逗号/中文逗号/空格/换行
    parts = re.split(r"[,\n，]+", s)
    out = []
    for p in parts:
        p = p.strip()
        if p:
            out.append(p.lower())
    # 去重保持顺序
    seen = set()
    uniq = []
    for k in out:
        if k not in seen:
            seen.add(k)
            uniq.append(k)
    return uniq

async def get_group_list_for_user(user_id: int) -> List[Tuple[int, str]]:
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT chat_id, COALESCE(title,'(no title)') AS title FROM tg_groups ORDER BY created_at DESC")
    # 只返回用户是管理员的群（需要调用TG接口确认）
    result = []
    for r in rows:
        chat_id = int(r["chat_id"])
        title = str(r["title"])
        try:
            member = await bot.get_chat_member(chat_id, user_id)
            if member.status in ("administrator", "creator"):
                result.append((chat_id, title))
        except Exception:
            pass
    return result

async def set_group_enabled(chat_id: int, enabled: bool):
    async with pool.acquire() as conn:
        await conn.execute("UPDATE tg_groups SET enabled=$2 WHERE chat_id=$1", chat_id, enabled)

async def set_work_keywords(chat_id: int, kind: str, keywords: List[str]):
    col = "work_in_keywords" if kind == "in" else "work_out_keywords"
    async with pool.acquire() as conn:
        await conn.execute(f"UPDATE group_keywords SET {col}=$2 WHERE chat_id=$1", chat_id, ", ".join(keywords))

async def get_work_keywords(chat_id: int) -> Tuple[List[str], List[str]]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT work_in_keywords, work_out_keywords FROM group_keywords WHERE chat_id=$1", chat_id)
    if not row:
        return (["上班","开工","in","start"], ["下班","off","end","收工"])
    return (split_keywords(row["work_in_keywords"]), split_keywords(row["work_out_keywords"]))

async def list_types(chat_id: int) -> List[Tuple[int, str]]:
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, type_name FROM group_types WHERE chat_id=$1 ORDER BY id ASC", chat_id)
    return [(int(r["id"]), str(r["type_name"])) for r in rows]

async def add_type(chat_id: int, type_name: str) -> str:
    type_name = type_name.strip()
    if not type_name:
        return "类型名不能为空"
    async with pool.acquire() as conn:
        cnt = await conn.fetchval("SELECT COUNT(*) FROM group_types WHERE chat_id=$1", chat_id)
        if cnt >= 5:
            return "已达到上限（最多5个类型）"
        try:
            await conn.execute("INSERT INTO group_types(chat_id, type_name) VALUES($1,$2)", chat_id, type_name)
        except asyncpg.UniqueViolationError:
            return "该类型已存在"
    return "OK"

async def del_type(chat_id: int, type_name: str) -> str:
    type_name = type_name.strip()
    async with pool.acquire() as conn:
        res = await conn.execute("DELETE FROM group_types WHERE chat_id=$1 AND type_name=$2", chat_id, type_name)
    return "OK" if res.endswith("1") else "未找到该类型"

async def set_type_keywords(chat_id: int, type_name: str, keywords: List[str]) -> str:
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT id FROM group_types WHERE chat_id=$1 AND type_name=$2", chat_id, type_name)
        if not row:
            return "未找到该类型，请先添加类型"
        type_id = int(row["id"])
        await conn.execute("DELETE FROM type_keywords WHERE type_id=$1", type_id)
        for k in keywords:
            await conn.execute("INSERT INTO type_keywords(type_id, keyword) VALUES($1,$2) ON CONFLICT DO NOTHING", type_id, k.lower())
    return "OK"

async def get_type_keywords_map(chat_id: int) -> List[Tuple[str, List[str]]]:
    async with pool.acquire() as conn:
        types = await conn.fetch("SELECT id, type_name FROM group_types WHERE chat_id=$1 ORDER BY id ASC", chat_id)
        out = []
        for t in types:
            kw = await conn.fetch("SELECT keyword FROM type_keywords WHERE type_id=$1 ORDER BY id ASC", int(t["id"]))
            out.append((str(t["type_name"]), [str(x["keyword"]).lower() for x in kw]))
        return out

async def insert_log(chat_id: int, user_id: int, user_name: str, action_type: str, raw_text: str):
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO attendance_logs(chat_id,user_id,user_name,action_type,raw_text) VALUES($1,$2,$3,$4,$5)",
            chat_id, user_id, user_name, action_type, raw_text
        )

async def export_logs_xlsx(chat_id: int, start_dt: datetime, end_dt: datetime) -> bytes:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT created_at, user_id, user_name, action_type, raw_text "
            "FROM attendance_logs WHERE chat_id=$1 AND created_at >= $2 AND created_at < $3 "
            "ORDER BY created_at ASC",
            chat_id, start_dt, end_dt
        )

    wb = Workbook()
    ws = wb.active
    ws.title = "logs"
    ws.append(["time", "user_id", "user_name", "action_type", "raw_text"])

    for r in rows:
        ws.append([
            r["created_at"].strftime("%Y-%m-%d %H:%M:%S"),
            int(r["user_id"]),
            r["user_name"] or "",
            r["action_type"],
            r["raw_text"] or ""
        ])

    bio = BytesIO()
    wb.save(bio)
    return bio.getvalue()

# ---------- Helpers ----------
def norm_text(s: str) -> str:
    s = (s or "").strip().lower()
    # 去掉多余标点（保留字母数字中文）
    s = re.sub(r"[^\w\u4e00-\u9fff\s]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def match_any(text: str, keywords: List[str]) -> bool:
    # 关键词包含匹配（更贴近你说的“吃饭/eat/抽/之类的”）
    for k in keywords:
        k = k.strip().lower()
        if not k:
            continue
        if k in text:
            return True
    return False

def kb_group_select(groups: List[Tuple[int, str]]) -> InlineKeyboardMarkup:
    buttons = []
    for chat_id, title in groups[:20]:
        buttons.append([InlineKeyboardButton(text=title, callback_data=f"pickgroup:{chat_id}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def kb_panel(chat_id: int, types: List[str]) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(text="✅ 上班", callback_data=f"act:{chat_id}:WORK_IN"),
            InlineKeyboardButton(text="✅ 下班", callback_data=f"act:{chat_id}:WORK_OUT"),
        ]
    ]
    # 类型按钮
    for t in types:
        rows.append([InlineKeyboardButton(text=f"⏸ {t}", callback_data=f"act:{chat_id}:BREAK:{t}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# ---------- State (简单做：每个用户选择一个当前群) ----------
USER_CTX = {}  # user_id -> chat_id

def ensure_admin(user_id: int) -> bool:
    return (not ADMIN_IDS) or (user_id in ADMIN_IDS)

# ---------- Handlers ----------
@dp.message(CommandStart())
async def start(m: Message):
    if not ensure_admin(m.from_user.id) and m.chat.type == "private":
        await m.reply("你没有权限配置该机器人。")
        return

    if m.chat.type == "private":
        await m.reply(
            "打卡机器人已启动。\n\n"
            "私聊指令：\n"
            "/groups 选择要配置的群\n"
            "/status 查看当前群配置\n"
            "/enable 开启本群打卡\n"
            "/disable 关闭本群打卡\n"
            "/addtype 类型名\n"
            "/deltype 类型名\n"
            "/settypekw 类型名 | 关键词1,关键词2,eat,...\n"
            "/setworkin 关键词1,关键词2,in,...\n"
            "/setworkout 关键词1,关键词2,off,...\n"
            "/export YYYY-MM-DD YYYY-MM-DD (可选，默认今天)\n\n"
            "群里指令：\n"
            "/panel 发送上班/下班按钮面板"
        )
    else:
        # 记录群
        await upsert_group(m.chat.id, m.chat.title or "")
        await m.reply("✅ 已加入群。管理员请私聊我 /start 进行配置。")

@dp.message(Command("groups"))
async def groups_cmd(m: Message):
    if m.chat.type != "private":
        return
    if not ensure_admin(m.from_user.id):
        await m.reply("你没有权限配置该机器人。")
        return

    glist = await get_group_list_for_user(m.from_user.id)
    if not glist:
        await m.reply("没找到你是管理员的群。先把机器人拉进群里，然后在群里发一句话让机器人记录群信息。")
        return
    await m.reply("请选择要配置的群：", reply_markup=kb_group_select(glist))

@dp.callback_query(F.data.startswith("pickgroup:"))
async def pick_group(cb: CallbackQuery):
    if not ensure_admin(cb.from_user.id):
        await cb.answer("无权限", show_alert=True)
        return
    chat_id = int(cb.data.split(":", 1)[1])
    USER_CTX[cb.from_user.id] = chat_id
    await cb.message.edit_text(f"✅ 已选择群：{chat_id}\n现在可以用 /status 查看配置。")
    await cb.answer()

def current_group(user_id: int) -> Optional[int]:
    return USER_CTX.get(user_id)

@dp.message(Command("status"))
async def status_cmd(m: Message):
    if m.chat.type != "private":
        return
    gid = current_group(m.from_user.id)
    if not gid:
        await m.reply("请先 /groups 选择一个群。")
        return

    enabled = await is_group_enabled(gid)
    win, wout = await get_work_keywords(gid)
    tmap = await get_type_keywords_map(gid)

    text = [
        f"当前群：{gid}",
        f"状态：{'✅开启' if enabled else '⛔关闭'}",
        f"上班关键词：{', '.join(win)}",
        f"下班关键词：{', '.join(wout)}",
        "类型（≤5）："
    ]
    if not tmap:
        text.append("（暂无）")
    else:
        for tn, kws in tmap:
            text.append(f"- {tn}: {', '.join(kws) if kws else '(无关键词)'}")
    await m.reply("\n".join(text))

@dp.message(Command("enable"))
async def enable_cmd(m: Message):
    if m.chat.type != "private":
        return
    gid = current_group(m.from_user.id)
    if not gid:
        await m.reply("请先 /groups 选择一个群。")
        return
    await set_group_enabled(gid, True)
    await m.reply("✅ 已开启该群打卡。")

@dp.message(Command("disable"))
async def disable_cmd(m: Message):
    if m.chat.type != "private":
        return
    gid = current_group(m.from_user.id)
    if not gid:
        await m.reply("请先 /groups 选择一个群。")
        return
    await set_group_enabled(gid, False)
    await m.reply("⛔ 已关闭该群打卡。")

@dp.message(Command("addtype"))
async def addtype_cmd(m: Message):
    if m.chat.type != "private":
        return
    gid = current_group(m.from_user.id)
    if not gid:
        await m.reply("请先 /groups 选择一个群。")
        return
    arg = m.text.split(maxsplit=1)
    if len(arg) < 2:
        await m.reply("用法：/addtype 吃饭")
        return
    res = await add_type(gid, arg[1])
    await m.reply("✅ 添加成功" if res == "OK" else f"❌ {res}")

@dp.message(Command("deltype"))
async def deltype_cmd(m: Message):
    if m.chat.type != "private":
        return
    gid = current_group(m.from_user.id)
    if not gid:
        await m.reply("请先 /groups 选择一个群。")
        return
    arg = m.text.split(maxsplit=1)
    if len(arg) < 2:
        await m.reply("用法：/deltype 吃饭")
        return
    res = await del_type(gid, arg[1])
    await m.reply("✅ 删除成功" if res == "OK" else f"❌ {res}")

@dp.message(Command("settypekw"))
async def settypekw_cmd(m: Message):
    if m.chat.type != "private":
        return
    gid = current_group(m.from_user.id)
    if not gid:
        await m.reply("请先 /groups 选择一个群。")
        return

    # 格式：/settypekw 吃饭 | 吃饭,eat,meal,饭
    raw = m.text[len("/settypekw"):].strip()
    if "|" not in raw:
        await m.reply("用法：/settypekw 吃饭 | 吃饭,eat,meal,饭")
        return
    type_name, kw_str = [x.strip() for x in raw.split("|", 1)]
    kws = split_keywords(kw_str)
    if not kws:
        await m.reply("关键词不能为空")
        return
    res = await set_type_keywords(gid, type_name, kws)
    await m.reply("✅ 已设置" if res == "OK" else f"❌ {res}")

@dp.message(Command("setworkin"))
async def setworkin_cmd(m: Message):
    if m.chat.type != "private":
        return
    gid = current_group(m.from_user.id)
    if not gid:
        await m.reply("请先 /groups 选择一个群。")
        return
    kw_str = m.text[len("/setworkin"):].strip()
    kws = split_keywords(kw_str)
    if not kws:
        await m.reply("用法：/setworkin 上班,开工,in,start")
        return
    await set_work_keywords(gid, "in", kws)
    await m.reply("✅ 已更新上班关键词")

@dp.message(Command("setworkout"))
async def setworkout_cmd(m: Message):
    if m.chat.type != "private":
        return
    gid = current_group(m.from_user.id)
    if not gid:
        await m.reply("请先 /groups 选择一个群。")
        return
    kw_str = m.text[len("/setworkout"):].strip()
    kws = split_keywords(kw_str)
    if not kws:
        await m.reply("用法：/setworkout 下班,off,end,收工")
        return
    await set_work_keywords(gid, "out", kws)
    await m.reply("✅ 已更新下班关键词")

@dp.message(Command("export"))
async def export_cmd(m: Message):
    if m.chat.type != "private":
        return
    gid = current_group(m.from_user.id)
    if not gid:
        await m.reply("请先 /groups 选择一个群。")
        return

    parts = m.text.split()
    # 默认导出今天
    if len(parts) == 1:
        start = date.today()
        end = start
    elif len(parts) == 3:
        start = datetime.strptime(parts[1], "%Y-%m-%d").date()
        end = datetime.strptime(parts[2], "%Y-%m-%d").date()
    else:
        await m.reply("用法：/export 或 /export 2026-02-01 2026-02-04")
        return

    start_dt = datetime.combine(start, datetime.min.time())
    end_dt = datetime.combine(end + timedelta(days=1), datetime.min.time())

    data = await export_logs_xlsx(gid, start_dt, end_dt)
    filename = f"attendance_{gid}_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.xlsx"
    await m.reply_document(BufferedInputFile(data, filename=filename), caption="✅ 导出完成")

@dp.message(Command("panel"))
async def panel_cmd(m: Message):
    # 只能在群里用
    if m.chat.type not in ("group", "supergroup"):
        return
    await upsert_group(m.chat.id, m.chat.title or "")

    # 只有管理员能发面板
    try:
        member = await bot.get_chat_member(m.chat.id, m.from_user.id)
        if member.status not in ("administrator", "creator"):
            await m.reply("只有管理员可以发送面板。")
            return
    except Exception:
        await m.reply("无法校验管理员权限。")
        return

    tlist = await list_types(m.chat.id)
    types = [t[1] for t in tlist]
    await m.reply("打卡面板：", reply_markup=kb_panel(m.chat.id, types))

@dp.callback_query(F.data.startswith("act:"))
async def act_cb(cb: CallbackQuery):
    # act:<chat_id>:WORK_IN 或 WORK_OUT 或 BREAK:<type>
    try:
        _, chat_id_str, action = cb.data.split(":", 2)
        chat_id = int(chat_id_str)
    except Exception:
        await cb.answer("参数错误", show_alert=True)
        return

    if not await is_group_enabled(chat_id):
        await cb.answer("该群已关闭打卡", show_alert=True)
        return

    u = cb.from_user
    user_name = (u.full_name or u.username or str(u.id))

    if action.startswith("BREAK:"):
        type_name = action.split(":", 1)[1]
        action_type = f"BREAK:{type_name}"
        raw_text = type_name
    else:
        action_type = action
        raw_text = action

    await insert_log(chat_id, u.id, user_name, action_type, raw_text)
    await cb.answer("✅ 已记录")

@dp.message(F.chat.type.in_(["group", "supergroup"]) & F.text)
async def group_text_listener(m: Message):
    await upsert_group(m.chat.id, m.chat.title or "")

    if not await is_group_enabled(m.chat.id):
        return

    text = norm_text(m.text)
    if not text:
        return

    win, wout = await get_work_keywords(m.chat.id)
    if match_any(text, win):
        await insert_log(m.chat.id, m.from_user.id, m.from_user.full_name, "WORK_IN", m.text)
        return
    if match_any(text, wout):
        await insert_log(m.chat.id, m.from_user.id, m.from_user.full_name, "WORK_OUT", m.text)
        return

    # 类型匹配
    tmap = await get_type_keywords_map(m.chat.id)
    for type_name, kws in tmap:
        if kws and match_any(text, kws):
            await insert_log(m.chat.id, m.from_user.id, m.from_user.full_name, f"BREAK:{type_name}", m.text)
            return

async def main():
    await db_init()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
