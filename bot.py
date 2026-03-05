import asyncio
import os
import sqlite3
import random
from itertools import combinations
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage


TOKEN = os.getenv("TOKEN")
ADMIN_IDS = set(int(x.strip())
                for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip())
BOT_USERNAME = "AfricaMixBot"

if not TOKEN:
    raise ValueError("TOKEN not found!")
if not ADMIN_IDS:
    raise ValueError("ADMIN_IDS not found!")

storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ---------- SQLite setup ----------
conn = sqlite3.connect("/data/bot.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    username TEXT,
    gender TEXT
)
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS meetings (
    meeting_id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id INTEGER,
    chat_id INTEGER,
    day TEXT,
    time TEXT,
    creator_id INTEGER,
    admin_message_id INTEGER
)
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS participants (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    meeting_id INTEGER,
    user_id INTEGER,
    display_name TEXT,
    pair_id INTEGER,
    gender TEXT,
    UNIQUE(meeting_id, user_id)
)
""")
conn.commit()

# ---------- FSM ----------


class CreateMeeting(StatesGroup):
    choosing_day = State()
    choosing_time = State()


class JoinMeeting(StatesGroup):
    choosing_solo_or_pair = State()
    choosing_partner = State()
    entering_guest_partner_name = State()
    entering_manual_partner_name = State()


class AddGuest(StatesGroup):
    entering_name = State()
    choosing_solo_or_pair = State()
    entering_partner_name = State()
    choosing_guest_gender = State()


class ManageGuest(StatesGroup):
    choosing_guest_to_delete = State()
    choosing_guest_for_pair = State()
    entering_pair_for_guest = State()
    choosing_pair_gender = State()


class LeaveConfirm(StatesGroup):
    choosing_leave_type = State()


class AdminAddUser(StatesGroup):
    waiting_for_user = State()


class AdminDeleteUser(StatesGroup):
    waiting_for_id = State()


class GameSetup(StatesGroup):
    choosing_courts = State()
    choosing_mode_4_1court = State()
    choosing_mode_6 = State()

# ---------- Helpers ----------


def get_meeting_id(message_id, chat_id):
    cursor.execute(
        "SELECT meeting_id FROM meetings WHERE message_id=? AND chat_id=?",
        (message_id, chat_id)
    )
    row = cursor.fetchone()
    return row[0] if row else None


def get_meeting_id_for_admin(message_id, chat_id, user_id):
    cursor.execute(
        "SELECT meeting_id FROM meetings WHERE message_id=? AND chat_id=?",
        (message_id, chat_id)
    )
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute(
        "SELECT meeting_id FROM meetings WHERE admin_message_id=? AND creator_id=?",
        (message_id, user_id)
    )
    row = cursor.fetchone()
    return row[0] if row else None


def next_pair_id(meeting_id):
    cursor.execute(
        "SELECT MAX(pair_id) FROM participants WHERE meeting_id=?", (meeting_id,))
    row = cursor.fetchone()
    return (row[0] or 0) + 1


def is_registered(meeting_id, user_id):
    cursor.execute(
        "SELECT id FROM participants WHERE meeting_id=? AND user_id=?", (meeting_id, user_id))
    return cursor.fetchone() is not None


def next_guest_id(meeting_id):
    cursor.execute(
        "SELECT MIN(user_id) FROM participants WHERE meeting_id=? AND user_id < 0", (meeting_id,))
    row = cursor.fetchone()
    return (row[0] or 0) - 1


def get_user_gender(user_id):
    cursor.execute("SELECT gender FROM users WHERE user_id=?", (user_id,))
    row = cursor.fetchone()
    return row[0] if row else "male"


def opposite_gender(gender):
    return "female" if gender == "male" else "male"


def get_meeting_header(meeting_id):
    cursor.execute(
        "SELECT day, time FROM meetings WHERE meeting_id=?", (meeting_id,))
    row = cursor.fetchone()
    if row:
        return f"📅 {row[0]} · 🕖 {row[1]}\n\n"
    return ""


def get_pairs_and_singles(meeting_id):
    cursor.execute("""
        SELECT pair_id, display_name, gender
        FROM participants
        WHERE meeting_id=? AND pair_id IS NOT NULL
        ORDER BY pair_id
    """, (meeting_id,))
    rows = cursor.fetchall()

    pair_dict = {}
    for pair_id, name, gender in rows:
        pair_dict.setdefault(pair_id, {})
        pair_dict[pair_id][gender] = name

    pairs_raw = []
    for pid, members in pair_dict.items():
        male = members.get("male", "?")
        female = members.get("female", "?")
        pairs_raw.append((male, female))

    cursor.execute("""
        SELECT display_name, gender
        FROM participants
        WHERE meeting_id=? AND pair_id IS NULL
    """, (meeting_id,))
    singles = cursor.fetchall()

    return pairs_raw, singles


def pairs_to_str(pairs_raw):
    return [f"{m} / {f}" for m, f in pairs_raw]


def single_note_text(singles):
    if not singles:
        return ""
    names = ", ".join(name for name, g in singles)
    return f"⚠️ Без пари: {names} — грає з тим хто сидить протилежної статі\n\n"


def deep_link_keyboard(meeting_id):
    """Клавіатура з кнопкою-посиланням для відкриття бота"""
    url = f"https://t.me/{BOT_USERNAME}?start=join_{meeting_id}"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="▶️ Відкрити бота і записатись", url=url)]
    ])


async def update_admin_message(bot, meeting_id):
    cursor.execute(
        "SELECT creator_id, admin_message_id FROM meetings WHERE meeting_id=?",
        (meeting_id,)
    )
    row = cursor.fetchone()
    if not row or not row[1]:
        return
    creator_id, admin_message_id = row
    text = format_text(meeting_id)
    try:
        await bot.edit_message_text(
            f"📋 Керування зустріччю:\n\n{text}",
            chat_id=creator_id,
            message_id=admin_message_id,
            reply_markup=admin_meeting_keyboard(meeting_id)
        )
    except Exception:
        pass

# ---------- Game schedule generators ----------


def schedule_3teams_sideout(meeting_id, pairs_str, singles, num_rounds=7):
    header = get_meeting_header(meeting_id)
    note = single_note_text(singles)
    lines = []
    for r in range(1, num_rounds + 1):
        p = pairs_str[:]
        random.shuffle(p)
        lines.append(f"<b>Раунд {r}</b>")
        lines.append(f"🏐 Корт 1: {p[0]} vs {p[1]} vs {p[2]}")
        lines.append("")
    lines.append(f"⏱ ~{num_rounds * 20} хв ({num_rounds} раундів × 20 хв)")
    return header + note + "\n".join(lines)


def schedule_4teams_roundrobin(meeting_id, pairs_str, singles):
    header = get_meeting_header(meeting_id)
    all_matches = list(combinations(range(4), 2))
    note = single_note_text(singles)
    lines = []
    last_sitting = set()
    used = []

    pool = all_matches * 3
    random.shuffle(pool)

    for match in pool:
        if len(used) >= 7:
            break
        sitting = tuple(i for i in range(4) if i not in match)
        if last_sitting and any(s in last_sitting for s in sitting):
            continue
        used.append((match, sitting))
        last_sitting = set(sitting)

    if len(used) < 7:
        for match in all_matches:
            if len(used) >= 7:
                break
            sitting = tuple(i for i in range(4) if i not in match)
            used.append((match, sitting))

    for r, (match, sitting) in enumerate(used[:7], 1):
        lines.append(f"<b>Раунд {r}</b>")
        lines.append(
            f"🏐 Корт 1: {pairs_str[match[0]]} vs {pairs_str[match[1]]}")
        lines.append(
            f"💤 Сидять: {pairs_str[sitting[0]]}, {pairs_str[sitting[1]]}")
        if singles:
            lines.append(
                f"   ↳ Без пари грає з тим хто сидить протилежної статі")
        lines.append("")
    lines.append(f"⏱ ~{7 * 20} хв (7 раундів × 20 хв)")
    return header + note + "\n".join(lines)


def schedule_4teams_2courts(meeting_id, pairs_str, singles):
    header = get_meeting_header(meeting_id)
    note = single_note_text(singles)
    lines = []
    for r in range(1, 8):
        p = pairs_str[:]
        random.shuffle(p)
        lines.append(f"<b>Раунд {r}</b>")
        lines.append(f"🏐 Корт 1: {p[0]} vs {p[1]}")
        lines.append(f"🏐 Корт 2: {p[2]} vs {p[3]}")
        lines.append("")
    lines.append(f"⏱ ~{7 * 20} хв (7 раундів × 20 хв)")
    return header + note + "\n".join(lines)


def schedule_5teams(meeting_id, pairs_str, singles, num_rounds=7):
    header = get_meeting_header(meeting_id)
    note = single_note_text(singles)
    lines = []
    for r in range(1, num_rounds + 1):
        p = pairs_str[:]
        random.shuffle(p)
        lines.append(f"<b>Раунд {r}</b>")
        lines.append(f"🏐 Корт 1 (сайдаут): {p[0]} vs {p[1]} vs {p[2]}")
        lines.append(f"🏐 Корт 2: {p[3]} vs {p[4]}")
        lines.append("")
    lines.append(f"⏱ ~{num_rounds * 20} хв ({num_rounds} раундів × 20 хв)")
    return header + note + "\n".join(lines)


def schedule_6teams_sideout(meeting_id, pairs_str, singles, num_rounds=7):
    header = get_meeting_header(meeting_id)
    note = single_note_text(singles)
    lines = []
    for r in range(1, num_rounds + 1):
        p = pairs_str[:]
        random.shuffle(p)
        lines.append(f"<b>Раунд {r}</b>")
        lines.append(f"🏐 Корт 1: {p[0]} vs {p[1]} vs {p[2]}")
        lines.append(f"🏐 Корт 2: {p[3]} vs {p[4]} vs {p[5]}")
        lines.append("")
    lines.append(f"⏱ ~{num_rounds * 20} хв ({num_rounds} раундів × 20 хв)")
    return header + note + "\n".join(lines)


def schedule_6teams_games(meeting_id, pairs_str, singles, num_rounds=7):
    header = get_meeting_header(meeting_id)
    note = single_note_text(singles)
    lines = []
    last_sitting = set()

    for r in range(1, num_rounds + 1):
        indices = list(range(6))
        candidates = [i for i in indices if i not in last_sitting]
        if len(candidates) < 2:
            candidates = indices
        sitting_idx = random.sample(candidates, 2)
        last_sitting = set(sitting_idx)
        playing_idx = [i for i in indices if i not in sitting_idx]
        random.shuffle(playing_idx)

        lines.append(f"<b>Раунд {r}</b>")
        lines.append(
            f"🏐 Корт 1: {pairs_str[playing_idx[0]]} vs {pairs_str[playing_idx[1]]}")
        lines.append(
            f"🏐 Корт 2: {pairs_str[playing_idx[2]]} vs {pairs_str[playing_idx[3]]}")
        lines.append(
            f"💤 Сидять: {pairs_str[sitting_idx[0]]}, {pairs_str[sitting_idx[1]]}")
        if singles:
            lines.append(
                f"   ↳ Без пари грає з тим хто сидить протилежної статі")
        lines.append("")

    lines.append(f"⏱ ~{num_rounds * 20} хв ({num_rounds} раундів × 20 хв)")
    return header + note + "\n".join(lines)


def schedule_7teams(meeting_id, pairs_str, singles, num_rounds=7):
    header = get_meeting_header(meeting_id)
    note = single_note_text(singles)
    lines = []
    for r in range(1, num_rounds + 1):
        p = pairs_str[:]
        random.shuffle(p)
        lines.append(f"<b>Раунд {r}</b>")
        lines.append(
            f"🏐 Корт 1 (сайдаут): {p[0]} vs {p[1]} vs {p[2]} vs {p[3]}")
        lines.append(f"🏐 Корт 2 (сайдаут): {p[4]} vs {p[5]} vs {p[6]}")
        lines.append("")
    lines.append(f"⏱ ~{num_rounds * 20} хв ({num_rounds} раундів × 20 хв)")
    return header + note + "\n".join(lines)


def schedule_8plus_teams(meeting_id, pairs_str, singles, num_rounds=7):
    header = get_meeting_header(meeting_id)
    note = single_note_text(singles)
    lines = []
    n = len(pairs_str)
    half = (n + 1) // 2

    for r in range(1, num_rounds + 1):
        p = pairs_str[:]
        random.shuffle(p)
        c1, c2 = p[:half], p[half:]
        playing1, waiting1 = c1[:3], c1[3:]
        playing2, waiting2 = c2[:3], c2[3:]
        lines.append(f"<b>Раунд {r}</b>")
        lines.append(f"🏐 Корт 1: {' vs '.join(playing1)}")
        if waiting1:
            lines.append(f"💤 Сидить (К1): {', '.join(waiting1)}")
        lines.append(f"🏐 Корт 2: {' vs '.join(playing2)}")
        if waiting2:
            lines.append(f"💤 Сидить (К2): {', '.join(waiting2)}")
        if singles:
            lines.append(
                f"   ↳ Без пари грає з тим хто сидить протилежної статі")
        lines.append("")

    lines.append(f"⏱ ~{num_rounds * 20} хв ({num_rounds} раундів × 20 хв)")
    return header + note + "\n".join(lines)


def generate_remix(meeting_id, pairs_raw, num_rounds=2):
    header = get_meeting_header(meeting_id)
    males = [p[0] for p in pairs_raw]
    females = [p[1] for p in pairs_raw]
    lines = ["<b>🔄 Міксовані пари</b>\n"]

    for r in range(1, num_rounds + 1):
        shuffled_males = males[:]
        shuffled_females = females[:]
        random.shuffle(shuffled_males)
        random.shuffle(shuffled_females)
        attempts = 0
        while attempts < 20:
            new_pairs = list(zip(shuffled_males, shuffled_females))
            if all(m != om or f != of for (m, f), (om, of) in zip(new_pairs, pairs_raw)):
                break
            random.shuffle(shuffled_males)
            random.shuffle(shuffled_females)
            attempts += 1

        lines.append(f"<b>Раунд {r}</b>")
        for i, (m, f) in enumerate(zip(shuffled_males, shuffled_females), 1):
            lines.append(f"  {i}. {m} / {f}")
        lines.append("")

    return header + "\n".join(lines)

# ---------- Keyboards ----------


def days_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="Понеділок", callback_data="day_Понеділок")],
        [InlineKeyboardButton(text="Вівторок",  callback_data="day_Вівторок")],
        [InlineKeyboardButton(text="Середа",    callback_data="day_Середа")],
        [InlineKeyboardButton(text="Четвер",    callback_data="day_Четвер")],
        [InlineKeyboardButton(text="Пятниця",   callback_data="day_Пятниця")],
        [InlineKeyboardButton(text="Субота",    callback_data="day_Субота")],
        [InlineKeyboardButton(text="Неділя",    callback_data="day_Неділя")],
    ])


def time_keyboard():
    times = [f"{h:02d}:00" for h in range(9, 21)]
    rows = [[InlineKeyboardButton(
        text=t, callback_data=f"time_{t}")] for t in times]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def courts_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1 корт",  callback_data="courts_1")],
        [InlineKeyboardButton(text="2 корти", callback_data="courts_2")],
    ])


def sideout_or_games_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="🏐 Сайдаути", callback_data="mode_sideout")],
        [InlineKeyboardButton(text="📋 Ігри",     callback_data="mode_games")],
    ])


def meeting_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏐 Записатись",
                              callback_data="join")],
        [InlineKeyboardButton(text="👫 Додати партнера",
                              callback_data="add_partner_from_base")],
        [InlineKeyboardButton(text="👤 Керування гостями",
                              callback_data="manage_guests")],
        [InlineKeyboardButton(text="❌ Скасувати запис",
                              callback_data="leave")],
    ])


def admin_meeting_keyboard(meeting_id=None):
    buttons = [
        [InlineKeyboardButton(text="🔀 Розбити по парах",
                              callback_data="shuffle_pairs")],
        [InlineKeyboardButton(text="📋 Розподіл ігор",
                              callback_data="game_distribute")],
        [InlineKeyboardButton(text="🔄 Міксанути пари",
                              callback_data="game_remix")],
        [InlineKeyboardButton(text="🗑 Видалити зустріч",
                              callback_data="delete")],
    ]

    if meeting_id:
        cursor.execute("""
            SELECT COUNT(DISTINCT pair_id) FROM participants
            WHERE meeting_id=? AND pair_id IS NOT NULL
        """, (meeting_id,))
        pair_count = cursor.fetchone()[0]

        cursor.execute("""
            SELECT COUNT(*) FROM participants
            WHERE meeting_id=? AND pair_id IS NULL
        """, (meeting_id,))
        single_count = cursor.fetchone()[0]

        if pair_count == 3 and single_count == 0:
            buttons.insert(0, [InlineKeyboardButton(
                text="🪑 Хто сидить першим?", callback_data="who_sits_first")])

    return InlineKeyboardMarkup(inline_keyboard=buttons)


def manage_guests_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Додати гостя (сам)",
                              callback_data="guest_add_solo")],
        [InlineKeyboardButton(text="➕ Додати гостя (парою)",
                              callback_data="guest_add_pair")],
        [InlineKeyboardButton(text="➕ Додати пару гостю",
                              callback_data="guest_add_partner")],
        [InlineKeyboardButton(text="❌ Видалити гостя",
                              callback_data="guest_delete")],
        [InlineKeyboardButton(text="◀️ Назад",
                              callback_data="guest_back")],
    ])


def solo_or_pair_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Сам(а)",
                              callback_data="join_solo")],
        [InlineKeyboardButton(text="З партнером з чату",
                              callback_data="join_pair")],
        [InlineKeyboardButton(text="З гостем",
                              callback_data="join_with_guest")],
    ])


def gender_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Хлопець", callback_data="gender_male")],
        [InlineKeyboardButton(text="Дівчина", callback_data="gender_female")],
    ])


def leave_type_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Тільки я",    callback_data="leave_solo")],
        [InlineKeyboardButton(text="Я і партнер", callback_data="leave_pair")],
    ])


def partners_keyboard(meeting_id, user_gender):
    needed_gender = opposite_gender(user_gender)

    cursor.execute("""
        SELECT u.user_id, u.first_name
        FROM users u
        WHERE u.gender=?
        AND u.user_id NOT IN (
            SELECT user_id FROM participants WHERE meeting_id=?
        )
    """, (needed_gender, meeting_id))
    from_base = cursor.fetchall()

    cursor.execute("""
        SELECT p.user_id, p.display_name
        FROM participants p
        WHERE p.meeting_id=? AND p.gender=? AND p.pair_id IS NULL
    """, (meeting_id, needed_gender))
    already_registered = cursor.fetchall()

    buttons = []
    for uid, name in from_base:
        buttons.append([InlineKeyboardButton(
            text=name, callback_data=f"pick_partner_{uid}")])
    for uid, name in already_registered:
        buttons.append([InlineKeyboardButton(
            text=f"{name} (вже записаний)", callback_data=f"pick_partner_{uid}")])

    buttons.append([InlineKeyboardButton(
        text="✍️ Інша людина (не з чату)", callback_data="pick_partner_guest")])
    buttons.append([InlineKeyboardButton(
        text="❌ Скасувати", callback_data="cancel_partner")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def guests_list_keyboard(meeting_id, action_prefix):
    cursor.execute("""
        SELECT user_id, display_name, pair_id
        FROM participants
        WHERE meeting_id=? AND user_id < 0
        ORDER BY id
    """, (meeting_id,))
    guests = cursor.fetchall()

    if not guests:
        return None

    buttons = []
    for uid, name, pair_id in guests:
        label = f"{name}" + (" (у парі)" if pair_id else " (сам)")
        buttons.append([InlineKeyboardButton(
            text=label, callback_data=f"{action_prefix}{uid}")])
    buttons.append([InlineKeyboardButton(text="❌ Скасувати",
                   callback_data="cancel_guest_action")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def admin_user_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Додати учасника",
                              callback_data="admin_add_user")],
        [InlineKeyboardButton(text="❌ Видалити учасника",
                              callback_data="admin_del_user")],
        [InlineKeyboardButton(text="📋 Показати всіх",
                              callback_data="admin_list_users")],
    ])

# ---------- Format meeting text ----------


def format_text(meeting_id):
    cursor.execute(
        "SELECT day, time FROM meetings WHERE meeting_id=?", (meeting_id,))
    meeting = cursor.fetchone()
    if not meeting:
        return "Зустріч скасована"
    day, time = meeting

    cursor.execute("""
        SELECT p.user_id, p.display_name, p.pair_id
        FROM participants p
        WHERE p.meeting_id=?
        ORDER BY p.pair_id NULLS LAST, p.id
    """, (meeting_id,))
    rows = cursor.fetchall()

    text = f"🌍 Африканці, граємо?\n\n📅 {day}\n🕖 {time}\n\nГравці:\n"

    if not rows:
        text += "Поки що тиша... хто перший? 😏"
        return text

    pairs = {}
    singles = []
    for user_id, display_name, pair_id in rows:
        if pair_id:
            pairs.setdefault(pair_id, []).append(display_name)
        else:
            singles.append(display_name)

    counter = 1
    for pid, names in pairs.items():
        if len(names) >= 2:
            text += f"{counter}. {names[0]} / {names[1]}\n"
        else:
            text += f"{counter}. {names[0]} (шукає пару)\n"
        counter += 1

    for name in singles:
        text += f"{counter}. {name}\n"
        counter += 1

    return text

# ---------- /start з deep link ----------


@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    args = message.text.split()
    # args[0] = "/start", args[1] = payload якщо є
    if len(args) < 2:
        await message.answer("👋 Привіт! Натисни кнопку в груповому чаті щоб записатись.")
        return

    payload = args[1]  # наприклад "join_42"

    if payload.startswith("join_"):
        try:
            meeting_id = int(payload.split("_")[1])
        except (IndexError, ValueError):
            await message.answer("❌ Невірне посилання.")
            return

        # Перевіряємо чи зустріч існує
        cursor.execute(
            "SELECT day, time FROM meetings WHERE meeting_id=?", (meeting_id,))
        meeting = cursor.fetchone()
        if not meeting:
            await message.answer("❌ Зустріч не знайдена або вже видалена.")
            return

        user = message.from_user
        cursor.execute(
            "SELECT first_name, gender FROM users WHERE user_id=?", (user.id,))
        row = cursor.fetchone()
        if not row:
            await message.answer("Тебе немає в базі. Попроси адміна додати тебе.")
            return

        if is_registered(meeting_id, user.id):
            await message.answer("Ти вже записаний(а)! ✅")
            return

        # Зберігаємо стан і питаємо як записатись
        cursor.execute(
            "SELECT chat_id, message_id FROM meetings WHERE meeting_id=?", (meeting_id,))
        m_row = cursor.fetchone()

        await state.update_data(
            meeting_id=meeting_id,
            user_id=user.id,
            display_name=row[0],
            user_gender=row[1],
            chat_id=m_row[0] if m_row else None,
            message_id=m_row[1] if m_row else None
        )
        await state.set_state(JoinMeeting.choosing_solo_or_pair)
        await message.answer(
            f"📅 {meeting[0]} · 🕖 {meeting[1]}\n\nЯк хочеш записатись?",
            reply_markup=solo_or_pair_keyboard()
        )

    elif payload.startswith("guests_"):
        try:
            meeting_id = int(payload.split("_")[1])
        except (IndexError, ValueError):
            await message.answer("❌ Невірне посилання.")
            return

        cursor.execute(
            "SELECT day, time FROM meetings WHERE meeting_id=?", (meeting_id,))
        meeting = cursor.fetchone()
        if not meeting:
            await message.answer("❌ Зустріч не знайдена або вже видалена.")
            return

        user_id = message.from_user.id
        cursor.execute("SELECT user_id FROM users WHERE user_id=?", (user_id,))
        if not cursor.fetchone():
            await message.answer("Тебе немає в базі. Попроси адміна додати тебе.")
            return

        cursor.execute(
            "SELECT chat_id, message_id FROM meetings WHERE meeting_id=?", (meeting_id,))
        m_row = cursor.fetchone()

        await state.update_data(
            meeting_id=meeting_id,
            chat_id=m_row[0] if m_row else None,
            message_id=m_row[1] if m_row else None
        )
        await message.answer(
            f"📅 {meeting[0]} · 🕖 {meeting[1]}\n\n👤 Керування гостями:",
            reply_markup=manage_guests_keyboard()
        )

    else:
        await message.answer("👋 Привіт! Натисни кнопку в груповому чаті щоб записатись.")

# ---------- Create meeting ----------


@dp.message(Command("create"))
async def create_meeting(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.delete()
    creator_id = message.from_user.id
    admin_state = dp.fsm.get_context(
        bot=message.bot, chat_id=creator_id, user_id=creator_id)
    await admin_state.update_data(chat_id=message.chat.id, creator_id=creator_id)
    await admin_state.set_state(CreateMeeting.choosing_day)
    await message.bot.send_message(creator_id, "📅 Обери день:", reply_markup=days_keyboard())


@dp.callback_query(F.data.startswith("day_"))
async def choose_day(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer()
        return
    day = callback.data.split("_", 1)[1]
    await state.update_data(day=day)
    await state.set_state(CreateMeeting.choosing_time)
    await callback.message.edit_text(f"📅 {day}\n\nТепер обери час:", reply_markup=time_keyboard())
    await callback.answer()


@dp.callback_query(F.data.startswith("time_"))
async def choose_time(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer()
        return
    data = await state.get_data()
    if "chat_id" not in data:
        await callback.answer("Помилка: почни знову через /create", show_alert=True)
        return
    time_val = callback.data.split("_", 1)[1]
    day = data["day"]
    chat_id = data["chat_id"]
    creator_id = callback.from_user.id

    sent = await callback.bot.send_message(chat_id, "⏳ Завантаження...", reply_markup=meeting_keyboard())
    cursor.execute(
        "INSERT INTO meetings(message_id, chat_id, day, time, creator_id) VALUES (?, ?, ?, ?, ?)",
        (sent.message_id, chat_id, day, time_val, creator_id)
    )
    conn.commit()
    meeting_id = cursor.lastrowid

    text = format_text(meeting_id)
    await sent.edit_text(text, reply_markup=meeting_keyboard())

    admin_msg = await callback.bot.send_message(
        creator_id,
        f"📋 Керування зустріччю:\n\n{text}",
        reply_markup=admin_meeting_keyboard(meeting_id)
    )
    cursor.execute(
        "UPDATE meetings SET admin_message_id=? WHERE meeting_id=?",
        (admin_msg.message_id, meeting_id)
    )
    conn.commit()

    await callback.message.edit_text("✅ Зустріч створена!")
    await state.clear()
    await callback.answer()

# ---------- Join ----------


@dp.callback_query(F.data == "join")
async def join_start(callback: CallbackQuery, state: FSMContext):
    user = callback.from_user
    cursor.execute(
        "SELECT first_name, gender FROM users WHERE user_id=?", (user.id,))
    row = cursor.fetchone()
    if not row:
        await callback.answer("Тебе немає в базі. Попроси адміна додати тебе.", show_alert=True)
        return

    meeting_id = get_meeting_id(
        callback.message.message_id, callback.message.chat.id)
    if not meeting_id:
        await callback.answer("Зустріч не знайдена.", show_alert=True)
        return

    if is_registered(meeting_id, user.id):
        await callback.answer("Ти вже записаний(а)! ✅", show_alert=True)
        return

    user_state = dp.fsm.get_context(
        bot=callback.bot, chat_id=user.id, user_id=user.id)
    await user_state.update_data(
        meeting_id=meeting_id,
        user_id=user.id,
        display_name=row[0],
        user_gender=row[1],
        chat_id=callback.message.chat.id,
        message_id=callback.message.message_id
    )
    await user_state.set_state(JoinMeeting.choosing_solo_or_pair)

    try:
        await callback.bot.send_message(
            user.id, "Як хочеш записатись?", reply_markup=solo_or_pair_keyboard())
    except Exception:
        # Юзер ще не писав боту — показуємо deep link
        await callback.answer(
            "Спочатку відкрий бота — натисни кнопку нижче 👇",
            show_alert=True
        )
        await user_state.clear()
        # Відправляємо в групу повідомлення з кнопкою (тільки цьому юзеру не можемо, тому alert)
        try:
            await callback.bot.send_message(
                callback.message.chat.id,
                f"👆 {user.first_name}, натисни щоб відкрити бота і записатись:",
                reply_markup=deep_link_keyboard(meeting_id)
            )
        except Exception:
            pass
        return
    await callback.answer()


@dp.callback_query(StateFilter(JoinMeeting.choosing_solo_or_pair), F.data == "join_solo")
async def join_solo(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    meeting_id = data["meeting_id"]

    cursor.execute(
        "INSERT OR IGNORE INTO participants(meeting_id, user_id, display_name, pair_id, gender) VALUES (?, ?, ?, NULL, ?)",
        (meeting_id, data["user_id"],
         data["display_name"], data["user_gender"])
    )
    conn.commit()

    await callback.bot.edit_message_text(
        format_text(meeting_id),
        chat_id=data["chat_id"],
        message_id=data["message_id"],
        reply_markup=meeting_keyboard()
    )
    await update_admin_message(callback.bot, meeting_id)
    await callback.message.edit_text("✅ Записано!")
    await state.clear()
    await callback.answer()


@dp.callback_query(StateFilter(JoinMeeting.choosing_solo_or_pair), F.data == "join_pair")
async def join_pair_choose(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    meeting_id = data["meeting_id"]
    user_gender = data["user_gender"]

    kb = partners_keyboard(meeting_id, user_gender)
    await state.set_state(JoinMeeting.choosing_partner)
    await callback.message.edit_text("Обери партнера/партнерку:", reply_markup=kb)
    await callback.answer()


@dp.callback_query(StateFilter(JoinMeeting.choosing_solo_or_pair), F.data == "join_with_guest")
async def join_with_guest_ask(callback: CallbackQuery, state: FSMContext):
    await state.set_state(JoinMeeting.entering_guest_partner_name)
    await callback.message.edit_text("Введи ім'я гостя з яким йдеш (буде автоматично іншої статі):")
    await callback.answer()


@dp.message(StateFilter(JoinMeeting.entering_guest_partner_name))
async def join_with_guest_save(message: Message, state: FSMContext):
    guest_name = message.text.strip()
    data = await state.get_data()
    meeting_id = data["meeting_id"]
    user_id = data["user_id"]
    display_name = data["display_name"]
    user_gender = data["user_gender"]
    guest_gender = opposite_gender(user_gender)

    pid = next_pair_id(meeting_id)
    guest_id = next_guest_id(meeting_id)

    cursor.execute(
        "INSERT OR IGNORE INTO participants(meeting_id, user_id, display_name, pair_id, gender) VALUES (?, ?, ?, ?, ?)",
        (meeting_id, user_id, display_name, pid, user_gender)
    )
    cursor.execute(
        "INSERT OR IGNORE INTO participants(meeting_id, user_id, display_name, pair_id, gender) VALUES (?, ?, ?, ?, ?)",
        (meeting_id, guest_id, guest_name, pid, guest_gender)
    )
    conn.commit()

    await message.bot.edit_message_text(
        format_text(meeting_id),
        chat_id=data["chat_id"],
        message_id=data["message_id"],
        reply_markup=meeting_keyboard()
    )
    await update_admin_message(message.bot, meeting_id)
    await message.answer(f"✅ Записано пару: {display_name} / {guest_name}!")
    await state.clear()


@dp.callback_query(StateFilter(JoinMeeting.choosing_partner), F.data == "pick_partner_guest")
async def pick_partner_guest_ask(callback: CallbackQuery, state: FSMContext):
    await state.set_state(JoinMeeting.entering_manual_partner_name)
    await callback.message.edit_text("Введи ім'я партнера/партнерки (буде автоматично іншої статі):")
    await callback.answer()


@dp.message(StateFilter(JoinMeeting.entering_manual_partner_name))
async def pick_partner_guest_save(message: Message, state: FSMContext):
    guest_name = message.text.strip()
    data = await state.get_data()
    meeting_id = data["meeting_id"]
    user_id = data["user_id"]
    display_name = data["display_name"]
    user_gender = data["user_gender"]
    guest_gender = opposite_gender(user_gender)

    pid = next_pair_id(meeting_id)
    guest_id = next_guest_id(meeting_id)

    if is_registered(meeting_id, user_id):
        cursor.execute(
            "UPDATE participants SET pair_id=? WHERE meeting_id=? AND user_id=?",
            (pid, meeting_id, user_id)
        )
    else:
        cursor.execute(
            "INSERT OR IGNORE INTO participants(meeting_id, user_id, display_name, pair_id, gender) VALUES (?, ?, ?, ?, ?)",
            (meeting_id, user_id, display_name, pid, user_gender)
        )

    cursor.execute(
        "INSERT OR IGNORE INTO participants(meeting_id, user_id, display_name, pair_id, gender) VALUES (?, ?, ?, ?, ?)",
        (meeting_id, guest_id, guest_name, pid, guest_gender)
    )
    conn.commit()

    await message.bot.edit_message_text(
        format_text(meeting_id),
        chat_id=data["chat_id"],
        message_id=data["message_id"],
        reply_markup=meeting_keyboard()
    )
    await update_admin_message(message.bot, meeting_id)
    await message.answer(f"✅ Записано пару: {display_name} / {guest_name}!")
    await state.clear()


@dp.callback_query(StateFilter(JoinMeeting.choosing_partner), F.data.startswith("pick_partner_"))
async def join_pair_save(callback: CallbackQuery, state: FSMContext):
    partner_id = int(callback.data.split("_")[2])
    data = await state.get_data()
    meeting_id = data["meeting_id"]
    user_id = data["user_id"]
    display_name = data["display_name"]
    user_gender = data["user_gender"]

    cursor.execute(
        "SELECT first_name, gender FROM users WHERE user_id=?", (partner_id,))
    p_row = cursor.fetchone()

    if p_row:
        partner_name, partner_gender = p_row
    else:
        cursor.execute(
            "SELECT display_name, gender FROM participants WHERE meeting_id=? AND user_id=?",
            (meeting_id, partner_id)
        )
        p_row2 = cursor.fetchone()
        if not p_row2:
            await callback.answer("Партнер не знайдений.", show_alert=True)
            return
        partner_name, partner_gender = p_row2

    pid = next_pair_id(meeting_id)

    if is_registered(meeting_id, user_id):
        cursor.execute(
            "UPDATE participants SET pair_id=? WHERE meeting_id=? AND user_id=?",
            (pid, meeting_id, user_id)
        )
    else:
        cursor.execute(
            "INSERT OR IGNORE INTO participants(meeting_id, user_id, display_name, pair_id, gender) VALUES (?, ?, ?, ?, ?)",
            (meeting_id, user_id, display_name, pid, user_gender)
        )

    if is_registered(meeting_id, partner_id):
        cursor.execute(
            "UPDATE participants SET pair_id=? WHERE meeting_id=? AND user_id=?",
            (pid, meeting_id, partner_id)
        )
    else:
        cursor.execute(
            "INSERT OR IGNORE INTO participants(meeting_id, user_id, display_name, pair_id, gender) VALUES (?, ?, ?, ?, ?)",
            (meeting_id, partner_id, partner_name, pid, partner_gender)
        )

    conn.commit()

    await callback.bot.edit_message_text(
        format_text(meeting_id),
        chat_id=data["chat_id"],
        message_id=data["message_id"],
        reply_markup=meeting_keyboard()
    )
    await update_admin_message(callback.bot, meeting_id)
    await callback.message.edit_text(f"✅ Записано пару: {display_name} / {partner_name}!")
    await state.clear()
    await callback.answer()


@dp.callback_query(StateFilter(JoinMeeting.choosing_partner), F.data == "cancel_partner")
async def cancel_partner(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("Скасовано.")
    await callback.answer()

# ---------- Add partner from base ----------


@dp.callback_query(F.data == "add_partner_from_base")
async def add_partner_from_base(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    meeting_id = get_meeting_id(
        callback.message.message_id, callback.message.chat.id)
    if not meeting_id:
        await callback.answer("Зустріч не знайдена.", show_alert=True)
        return
    if not is_registered(meeting_id, user_id):
        await callback.answer("Спочатку запишись на зустріч 🏐", show_alert=True)
        return

    cursor.execute(
        "SELECT pair_id FROM participants WHERE meeting_id=? AND user_id=?", (meeting_id, user_id))
    row = cursor.fetchone()
    if row and row[0]:
        await callback.answer("Ти вже в парі! Спочатку скасуй запис ❌", show_alert=True)
        return

    user_gender = get_user_gender(user_id)
    kb = partners_keyboard(meeting_id, user_gender)

    cursor.execute("SELECT first_name FROM users WHERE user_id=?", (user_id,))
    name_row = cursor.fetchone()

    user_state = dp.fsm.get_context(
        bot=callback.bot, chat_id=user_id, user_id=user_id)
    await user_state.update_data(
        meeting_id=meeting_id,
        user_id=user_id,
        display_name=name_row[0] if name_row else "",
        user_gender=user_gender,
        chat_id=callback.message.chat.id,
        message_id=callback.message.message_id
    )
    await user_state.set_state(JoinMeeting.choosing_partner)

    try:
        await callback.bot.send_message(user_id, "Обери партнера/партнерку:", reply_markup=kb)
    except Exception:
        await callback.answer(
            "Спочатку відкрий бота — натисни кнопку нижче 👇",
            show_alert=True
        )
        await user_state.clear()
        try:
            await callback.bot.send_message(
                callback.message.chat.id,
                f"👆 {callback.from_user.first_name}, натисни щоб відкрити бота:",
                reply_markup=deep_link_keyboard(meeting_id)
            )
        except Exception:
            pass
        return
    await callback.answer()

# ---------- Manage guests ----------


@dp.callback_query(F.data == "manage_guests")
async def manage_guests(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    cursor.execute("SELECT user_id FROM users WHERE user_id=?", (user_id,))
    if not cursor.fetchone():
        await callback.answer("Тебе немає в базі. Попроси адміна додати тебе.", show_alert=True)
        return

    meeting_id = get_meeting_id(
        callback.message.message_id, callback.message.chat.id)
    if not meeting_id:
        await callback.answer("Зустріч не знайдена.", show_alert=True)
        return

    user_state = dp.fsm.get_context(
        bot=callback.bot, chat_id=user_id, user_id=user_id)
    await user_state.update_data(
        meeting_id=meeting_id,
        chat_id=callback.message.chat.id,
        message_id=callback.message.message_id
    )

    try:
        await callback.bot.send_message(
            user_id, "👤 Керування гостями:", reply_markup=manage_guests_keyboard())
    except Exception:
        await callback.answer(
            "Спочатку відкрий бота — натисни кнопку нижче 👇",
            show_alert=True
        )
        try:
            await callback.bot.send_message(
                callback.message.chat.id,
                f"👆 {callback.from_user.first_name}, натисни щоб відкрити бота:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(
                        text="▶️ Відкрити бота",
                        url=f"https://t.me/{BOT_USERNAME}?start=guests_{meeting_id}"
                    )
                ]])
            )
        except Exception:
            pass
        return
    await callback.answer()


@dp.callback_query(F.data == "guest_back")
async def guest_back(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("Скасовано.")
    await callback.answer()


@dp.callback_query(F.data == "cancel_guest_action")
async def cancel_guest_action(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("Скасовано.")
    await callback.answer()


@dp.callback_query(F.data == "guest_add_solo")
async def guest_add_solo(callback: CallbackQuery, state: FSMContext):
    user_state = dp.fsm.get_context(
        bot=callback.bot, chat_id=callback.from_user.id, user_id=callback.from_user.id)
    await user_state.update_data(guest_mode="solo")
    await user_state.set_state(AddGuest.entering_name)
    await callback.message.edit_text("👤 Введи ім'я гостя:")
    await callback.answer()


@dp.callback_query(F.data == "guest_add_pair")
async def guest_add_pair(callback: CallbackQuery, state: FSMContext):
    user_state = dp.fsm.get_context(
        bot=callback.bot, chat_id=callback.from_user.id, user_id=callback.from_user.id)
    await user_state.update_data(guest_mode="pair")
    await user_state.set_state(AddGuest.entering_name)
    await callback.message.edit_text("👤 Введи ім'я першого гостя:")
    await callback.answer()


@dp.message(StateFilter(AddGuest.entering_name))
async def add_guest_name(message: Message, state: FSMContext):
    data = await state.get_data()
    await state.update_data(guest_name=message.text.strip())
    mode = data.get("guest_mode", "solo")
    if mode == "pair":
        await state.set_state(AddGuest.entering_partner_name)
        await message.answer("Введи ім'я партнера гостя:")
    else:
        await state.set_state(AddGuest.choosing_guest_gender)
        await message.answer(f"Стать для {message.text.strip()}?", reply_markup=gender_keyboard())


@dp.message(StateFilter(AddGuest.entering_partner_name))
async def add_guest_pair_partner_name(message: Message, state: FSMContext):
    await state.update_data(partner_name=message.text.strip())
    await state.set_state(AddGuest.choosing_guest_gender)
    data = await state.get_data()
    await message.answer(f"Стать для {data['guest_name']}?", reply_markup=gender_keyboard())


@dp.callback_query(StateFilter(AddGuest.choosing_guest_gender), F.data.startswith("gender_"))
async def add_guest_gender_save(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    gender = callback.data.split("_")[1]
    meeting_id = data["meeting_id"]
    guest_name = data["guest_name"]

    if "partner_name" in data:
        gender2 = opposite_gender(gender)
        partner_name = data["partner_name"]
        pid = next_pair_id(meeting_id)
        id1 = next_guest_id(meeting_id)
        cursor.execute(
            "INSERT INTO participants(meeting_id, user_id, display_name, pair_id, gender) VALUES (?, ?, ?, ?, ?)",
            (meeting_id, id1, guest_name, pid, gender)
        )
        id2 = next_guest_id(meeting_id)
        cursor.execute(
            "INSERT INTO participants(meeting_id, user_id, display_name, pair_id, gender) VALUES (?, ?, ?, ?, ?)",
            (meeting_id, id2, partner_name, pid, gender2)
        )
        conn.commit()
        await callback.bot.edit_message_text(
            format_text(meeting_id),
            chat_id=data["chat_id"],
            message_id=data["message_id"],
            reply_markup=meeting_keyboard()
        )
        await update_admin_message(callback.bot, meeting_id)
        await callback.message.edit_text(f"✅ Додано пару: {guest_name} / {partner_name}!")
    else:
        fake_id = next_guest_id(meeting_id)
        cursor.execute(
            "INSERT INTO participants(meeting_id, user_id, display_name, pair_id, gender) VALUES (?, ?, ?, NULL, ?)",
            (meeting_id, fake_id, guest_name, gender)
        )
        conn.commit()
        await callback.bot.edit_message_text(
            format_text(meeting_id),
            chat_id=data["chat_id"],
            message_id=data["message_id"],
            reply_markup=meeting_keyboard()
        )
        await update_admin_message(callback.bot, meeting_id)
        await callback.message.edit_text(f"✅ Додано гостя: {guest_name}!")

    await state.clear()
    await callback.answer()


@dp.callback_query(F.data == "guest_add_partner")
async def guest_add_partner_start(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    user_state = dp.fsm.get_context(
        bot=callback.bot, chat_id=user_id, user_id=user_id)
    data = await user_state.get_data()
    meeting_id = data.get("meeting_id")
    if not meeting_id:
        await callback.answer("Зустріч не знайдена.", show_alert=True)
        return

    cursor.execute("""
        SELECT user_id, display_name, gender
        FROM participants
        WHERE meeting_id=? AND user_id < 0 AND pair_id IS NULL
    """, (meeting_id,))
    solo_guests = cursor.fetchall()

    if not solo_guests:
        await callback.answer("Немає гостей без пари.", show_alert=True)
        return

    buttons = []
    for uid, name, g in solo_guests:
        buttons.append([InlineKeyboardButton(
            text=name, callback_data=f"guestpair_{uid}")])
    buttons.append([InlineKeyboardButton(text="❌ Скасувати",
                   callback_data="cancel_guest_action")])
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)

    await user_state.set_state(ManageGuest.choosing_guest_for_pair)
    await callback.message.edit_text("Обери гостя якому додати пару:", reply_markup=kb)
    await callback.answer()


@dp.callback_query(StateFilter(ManageGuest.choosing_guest_for_pair), F.data.startswith("guestpair_"))
async def guest_pair_chosen(callback: CallbackQuery, state: FSMContext):
    guest_id = int(callback.data.split("_")[1])
    user_state = dp.fsm.get_context(
        bot=callback.bot, chat_id=callback.from_user.id, user_id=callback.from_user.id)
    await user_state.update_data(target_guest_id=guest_id)
    await user_state.set_state(ManageGuest.entering_pair_for_guest)
    await callback.message.edit_text("Введи ім'я партнера для цього гостя:")
    await callback.answer()


@dp.message(StateFilter(ManageGuest.entering_pair_for_guest))
async def guest_pair_name_entered(message: Message, state: FSMContext):
    partner_name = message.text.strip()
    data = await state.get_data()
    meeting_id = data["meeting_id"]
    guest_id = data["target_guest_id"]

    cursor.execute("SELECT gender, display_name FROM participants WHERE meeting_id=? AND user_id=?",
                   (meeting_id, guest_id))
    row = cursor.fetchone()
    if not row:
        await message.answer("Гість не знайдений.")
        await state.clear()
        return

    guest_gender, guest_name = row
    partner_gender = opposite_gender(guest_gender)
    pid = next_pair_id(meeting_id)
    partner_id = next_guest_id(meeting_id)

    cursor.execute(
        "UPDATE participants SET pair_id=? WHERE meeting_id=? AND user_id=?",
        (pid, meeting_id, guest_id)
    )
    cursor.execute(
        "INSERT INTO participants(meeting_id, user_id, display_name, pair_id, gender) VALUES (?, ?, ?, ?, ?)",
        (meeting_id, partner_id, partner_name, pid, partner_gender)
    )
    conn.commit()

    await message.bot.edit_message_text(
        format_text(meeting_id),
        chat_id=data["chat_id"],
        message_id=data["message_id"],
        reply_markup=meeting_keyboard()
    )
    await update_admin_message(message.bot, meeting_id)
    await message.answer(f"✅ Пару додано: {guest_name} / {partner_name}!")
    await state.clear()


@dp.callback_query(F.data == "guest_delete")
async def guest_delete_start(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    user_state = dp.fsm.get_context(
        bot=callback.bot, chat_id=user_id, user_id=user_id)
    data = await user_state.get_data()
    meeting_id = data.get("meeting_id")
    if not meeting_id:
        await callback.answer("Зустріч не знайдена.", show_alert=True)
        return

    kb = guests_list_keyboard(meeting_id, "guestdel_")
    if not kb:
        await callback.answer("Немає гостей для видалення.", show_alert=True)
        return

    await user_state.set_state(ManageGuest.choosing_guest_to_delete)
    await callback.message.edit_text("Кого видалити?", reply_markup=kb)
    await callback.answer()


@dp.callback_query(StateFilter(ManageGuest.choosing_guest_to_delete), F.data.startswith("guestdel_"))
async def guest_delete_confirm(callback: CallbackQuery, state: FSMContext):
    guest_id = int(callback.data.split("_")[1])
    user_state = dp.fsm.get_context(
        bot=callback.bot, chat_id=callback.from_user.id, user_id=callback.from_user.id)
    data = await user_state.get_data()
    meeting_id = data["meeting_id"]

    cursor.execute(
        "SELECT pair_id, display_name FROM participants WHERE meeting_id=? AND user_id=?",
        (meeting_id, guest_id)
    )
    row = cursor.fetchone()
    if not row:
        await callback.answer("Гість не знайдений.", show_alert=True)
        await state.clear()
        return

    pair_id, guest_name = row

    if pair_id:
        cursor.execute(
            "DELETE FROM participants WHERE meeting_id=? AND pair_id=?", (meeting_id, pair_id))
        msg = f"✅ Видалено пару з гостем {guest_name}."
    else:
        cursor.execute(
            "DELETE FROM participants WHERE meeting_id=? AND user_id=?", (meeting_id, guest_id))
        msg = f"✅ Гостя {guest_name} видалено."

    conn.commit()

    await callback.bot.edit_message_text(
        format_text(meeting_id),
        chat_id=data["chat_id"],
        message_id=data["message_id"],
        reply_markup=meeting_keyboard()
    )
    await update_admin_message(callback.bot, meeting_id)
    await callback.message.edit_text(msg)
    await state.clear()
    await callback.answer()

# ---------- Leave ----------


@dp.callback_query(F.data == "leave")
async def leave_start(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    meeting_id = get_meeting_id(
        callback.message.message_id, callback.message.chat.id)
    if not meeting_id:
        await callback.answer("Зустріч не знайдена.", show_alert=True)
        return

    cursor.execute(
        "SELECT pair_id FROM participants WHERE meeting_id=? AND user_id=?", (meeting_id, user_id))
    row = cursor.fetchone()
    if not row:
        await callback.answer("Тебе немає в списку.", show_alert=True)
        return

    pair_id = row[0]

    if pair_id:
        user_state = dp.fsm.get_context(
            bot=callback.bot, chat_id=user_id, user_id=user_id)
        await user_state.update_data(
            meeting_id=meeting_id,
            user_id=user_id,
            pair_id=pair_id,
            message_id=callback.message.message_id,
            chat_id=callback.message.chat.id
        )
        await user_state.set_state(LeaveConfirm.choosing_leave_type)
        try:
            await callback.bot.send_message(
                user_id, "Ти в парі. Що скасувати?", reply_markup=leave_type_keyboard())
        except Exception:
            await callback.answer(
                "Спочатку відкрий бота — натисни кнопку нижче 👇",
                show_alert=True
            )
            await user_state.clear()
            try:
                await callback.bot.send_message(
                    callback.message.chat.id,
                    f"👆 {callback.from_user.first_name}, натисни щоб відкрити бота:",
                    reply_markup=deep_link_keyboard(meeting_id)
                )
            except Exception:
                pass
            return
    else:
        cursor.execute(
            "DELETE FROM participants WHERE meeting_id=? AND user_id=?", (meeting_id, user_id))
        conn.commit()
        await callback.message.edit_text(format_text(meeting_id), reply_markup=meeting_keyboard())
        await update_admin_message(callback.bot, meeting_id)
        await callback.answer("✅ Запис скасовано")


@dp.callback_query(StateFilter(LeaveConfirm.choosing_leave_type), F.data == "leave_solo")
async def leave_solo_confirm(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    meeting_id = data["meeting_id"]
    user_id = data["user_id"]
    pair_id = data["pair_id"]

    cursor.execute(
        "DELETE FROM participants WHERE meeting_id=? AND user_id=?", (meeting_id, user_id))
    cursor.execute(
        "UPDATE participants SET pair_id=NULL WHERE meeting_id=? AND pair_id=?", (meeting_id, pair_id))
    conn.commit()

    await callback.bot.edit_message_text(
        format_text(meeting_id),
        chat_id=data["chat_id"],
        message_id=data["message_id"],
        reply_markup=meeting_keyboard()
    )
    await update_admin_message(callback.bot, meeting_id)
    await callback.message.edit_text("✅ Тільки твій запис скасовано. Партнер залишився.")
    await state.clear()
    await callback.answer()


@dp.callback_query(StateFilter(LeaveConfirm.choosing_leave_type), F.data == "leave_pair")
async def leave_pair_confirm(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    meeting_id = data["meeting_id"]
    pair_id = data["pair_id"]

    cursor.execute(
        "DELETE FROM participants WHERE meeting_id=? AND pair_id=?", (meeting_id, pair_id))
    conn.commit()

    await callback.bot.edit_message_text(
        format_text(meeting_id),
        chat_id=data["chat_id"],
        message_id=data["message_id"],
        reply_markup=meeting_keyboard()
    )
    await update_admin_message(callback.bot, meeting_id)
    await callback.message.edit_text("✅ Пару скасовано повністю.")
    await state.clear()
    await callback.answer()

# ---------- Shuffle pairs ----------


@dp.callback_query(F.data == "shuffle_pairs")
async def shuffle_pairs(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Тільки адмін може розбивати по парах", show_alert=True)
        return

    meeting_id = get_meeting_id_for_admin(
        callback.message.message_id, callback.message.chat.id, callback.from_user.id)
    if not meeting_id:
        await callback.answer("Зустріч не знайдена.", show_alert=True)
        return

    cursor.execute("""
        SELECT user_id, display_name, gender
        FROM participants
        WHERE meeting_id=? AND pair_id IS NULL
    """, (meeting_id,))
    singles = cursor.fetchall()

    males = [(uid, name, g) for uid, name, g in singles if g == "male"]
    females = [(uid, name, g) for uid, name, g in singles if g == "female"]

    if not males and not females:
        await callback.answer("Немає одиночних гравців.", show_alert=True)
        return

    if not males or not females:
        await callback.answer(
            "Усі одиночні гравці однієї статі — пари неможливо створити 😔",
            show_alert=True
        )
        return

    random.shuffle(males)
    random.shuffle(females)

    paired = 0
    while males and females:
        m = males.pop()
        f = females.pop()
        pid = next_pair_id(meeting_id)
        cursor.execute(
            "UPDATE participants SET pair_id=? WHERE meeting_id=? AND user_id=?",
            (pid, meeting_id, m[0]))
        cursor.execute(
            "UPDATE participants SET pair_id=? WHERE meeting_id=? AND user_id=?",
            (pid, meeting_id, f[0]))
        paired += 1

    conn.commit()

    await callback.message.edit_text(
        f"📋 Керування зустріччю:\n\n{format_text(meeting_id)}",
        reply_markup=admin_meeting_keyboard(meeting_id)
    )

    cursor.execute(
        "SELECT chat_id, message_id FROM meetings WHERE meeting_id=?", (meeting_id,))
    row = cursor.fetchone()
    if row:
        try:
            await callback.bot.edit_message_text(
                format_text(meeting_id),
                chat_id=row[0],
                message_id=row[1],
                reply_markup=meeting_keyboard()
            )
        except Exception:
            pass

    await callback.answer(f"✅ Створено {paired} пар!")

# ---------- Game distribute ----------


@dp.callback_query(F.data == "game_distribute")
async def game_distribute(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Тільки адмін", show_alert=True)
        return

    meeting_id = get_meeting_id_for_admin(
        callback.message.message_id, callback.message.chat.id, callback.from_user.id)
    if not meeting_id:
        await callback.answer("Зустріч не знайдена.", show_alert=True)
        return

    pairs_raw, singles = get_pairs_and_singles(meeting_id)
    n = len(pairs_raw)

    if n == 0:
        await callback.answer("Спочатку сформуй пари!", show_alert=True)
        return

    cursor.execute(
        "SELECT chat_id FROM meetings WHERE meeting_id=?", (meeting_id,))
    chat_row = cursor.fetchone()
    if not chat_row:
        await callback.answer("Зустріч не знайдена.", show_alert=True)
        return
    group_chat_id = chat_row[0]
    pairs_str = pairs_to_str(pairs_raw)
    admin_id = callback.from_user.id

    if n == 2:
        await callback.answer("2 команди — просто грайте між собою 😊", show_alert=True)
        return

    if n == 3:
        text = schedule_3teams_sideout(meeting_id, pairs_str, singles)
        await callback.bot.send_message(group_chat_id, text, parse_mode="HTML")
        await callback.answer("✅ Розклад опубліковано!")
        return

    if n == 4:
        admin_state = dp.fsm.get_context(
            bot=callback.bot, chat_id=admin_id, user_id=admin_id)
        await admin_state.update_data(meeting_id=meeting_id, group_chat_id=group_chat_id)
        await admin_state.set_state(GameSetup.choosing_courts)
        await callback.bot.send_message(
            admin_id, "4 команди — скільки кортів?", reply_markup=courts_keyboard())
        await callback.answer()
        return

    if n == 5:
        text = schedule_5teams(meeting_id, pairs_str, singles)
        await callback.bot.send_message(group_chat_id, text, parse_mode="HTML")
        await callback.answer("✅ Розклад опубліковано!")
        return

    if n == 6:
        admin_state = dp.fsm.get_context(
            bot=callback.bot, chat_id=admin_id, user_id=admin_id)
        await admin_state.update_data(meeting_id=meeting_id, group_chat_id=group_chat_id)
        await admin_state.set_state(GameSetup.choosing_mode_6)
        await callback.bot.send_message(
            admin_id, "6 команд — який формат?", reply_markup=sideout_or_games_keyboard())
        await callback.answer()
        return

    if n == 7:
        text = schedule_7teams(meeting_id, pairs_str, singles)
        await callback.bot.send_message(group_chat_id, text, parse_mode="HTML")
        await callback.answer("✅ Розклад опубліковано!")
        return

    text = schedule_8plus_teams(meeting_id, pairs_str, singles)
    await callback.bot.send_message(group_chat_id, text, parse_mode="HTML")
    await callback.answer("✅ Розклад опубліковано!")


@dp.callback_query(StateFilter(GameSetup.choosing_courts), F.data.startswith("courts_"))
async def game_4teams_courts(callback: CallbackQuery, state: FSMContext):
    num_courts = int(callback.data.split("_")[1])
    data = await state.get_data()
    meeting_id = data["meeting_id"]
    group_chat_id = data["group_chat_id"]

    pairs_raw, singles = get_pairs_and_singles(meeting_id)
    pairs_str = pairs_to_str(pairs_raw)

    if num_courts == 2:
        text = schedule_4teams_2courts(meeting_id, pairs_str, singles)
        await callback.bot.send_message(group_chat_id, text, parse_mode="HTML")
        await callback.message.edit_text("✅ Розклад опубліковано!")
        await state.clear()
        await callback.answer()
        return

    await state.set_state(GameSetup.choosing_mode_4_1court)
    await callback.message.edit_text(
        "4 команди, 1 корт — який формат?", reply_markup=sideout_or_games_keyboard())
    await callback.answer()


@dp.callback_query(StateFilter(GameSetup.choosing_mode_4_1court), F.data.startswith("mode_"))
async def game_4teams_1court_mode(callback: CallbackQuery, state: FSMContext):
    mode = callback.data.split("_")[1]
    data = await state.get_data()
    meeting_id = data["meeting_id"]
    group_chat_id = data["group_chat_id"]

    pairs_raw, singles = get_pairs_and_singles(meeting_id)
    pairs_str = pairs_to_str(pairs_raw)

    if mode == "sideout":
        await callback.message.edit_text(
            "✅ Сайдаути! Всі 4 команди на 1 корті — розклад не потрібен 😊")
    else:
        text = schedule_4teams_roundrobin(meeting_id, pairs_str, singles)
        await callback.bot.send_message(group_chat_id, text, parse_mode="HTML")
        await callback.message.edit_text("✅ Розклад опубліковано!")

    await state.clear()
    await callback.answer()


@dp.callback_query(StateFilter(GameSetup.choosing_mode_6), F.data.startswith("mode_"))
async def game_6teams_mode(callback: CallbackQuery, state: FSMContext):
    mode = callback.data.split("_")[1]
    data = await state.get_data()
    meeting_id = data["meeting_id"]
    group_chat_id = data["group_chat_id"]

    pairs_raw, singles = get_pairs_and_singles(meeting_id)
    pairs_str = pairs_to_str(pairs_raw)

    if mode == "sideout":
        text = schedule_6teams_sideout(meeting_id, pairs_str, singles)
    else:
        text = schedule_6teams_games(meeting_id, pairs_str, singles)

    await callback.bot.send_message(group_chat_id, text, parse_mode="HTML")
    await callback.message.edit_text("✅ Розклад опубліковано!")
    await state.clear()
    await callback.answer()

# ---------- Who sits first ----------


@dp.callback_query(F.data == "who_sits_first")
async def who_sits_first(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Тільки адмін", show_alert=True)
        return

    meeting_id = get_meeting_id_for_admin(
        callback.message.message_id, callback.message.chat.id, callback.from_user.id)
    if not meeting_id:
        await callback.answer("Зустріч не знайдена.", show_alert=True)
        return

    pairs_raw, _ = get_pairs_and_singles(meeting_id)
    if not pairs_raw:
        await callback.answer("Немає пар.", show_alert=True)
        return

    sitting = random.choice(pairs_raw)
    cursor.execute(
        "SELECT chat_id FROM meetings WHERE meeting_id=?", (meeting_id,))
    row = cursor.fetchone()
    if row:
        header = get_meeting_header(meeting_id)
        text = f"{header}🪑 Перший раунд сидить: <b>{sitting[0]} / {sitting[1]}</b>"
        await callback.bot.send_message(row[0], text, parse_mode="HTML")
    await callback.answer()

# ---------- Remix ----------


@dp.callback_query(F.data == "game_remix")
async def game_remix(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Тільки адмін", show_alert=True)
        return

    meeting_id = get_meeting_id_for_admin(
        callback.message.message_id, callback.message.chat.id, callback.from_user.id)
    if not meeting_id:
        await callback.answer("Зустріч не знайдена.", show_alert=True)
        return

    pairs_raw, _ = get_pairs_and_singles(meeting_id)
    if len(pairs_raw) < 2:
        await callback.answer("Потрібно мінімум 2 пари для міксу!", show_alert=True)
        return

    cursor.execute(
        "SELECT chat_id FROM meetings WHERE meeting_id=?", (meeting_id,))
    row = cursor.fetchone()
    if row:
        text = generate_remix(meeting_id, pairs_raw)
        await callback.bot.send_message(row[0], text, parse_mode="HTML")
    await callback.answer()

# ---------- Delete meeting ----------


@dp.callback_query(F.data == "delete")
async def delete_meeting(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Тільки адмін може видаляти", show_alert=True)
        return

    meeting_id = get_meeting_id_for_admin(
        callback.message.message_id, callback.message.chat.id, callback.from_user.id)
    if meeting_id:
        cursor.execute(
            "SELECT chat_id, message_id, admin_message_id, creator_id FROM meetings WHERE meeting_id=?",
            (meeting_id,)
        )
        row = cursor.fetchone()
        cursor.execute(
            "DELETE FROM meetings WHERE meeting_id=?", (meeting_id,))
        cursor.execute(
            "DELETE FROM participants WHERE meeting_id=?", (meeting_id,))
        conn.commit()

        if row:
            group_chat_id, group_message_id, admin_message_id, creator_id = row
            try:
                await callback.bot.delete_message(group_chat_id, group_message_id)
            except Exception:
                pass
            try:
                await callback.bot.edit_message_text(
                    "🗑 Зустріч видалена.",
                    chat_id=creator_id,
                    message_id=admin_message_id
                )
            except Exception:
                pass
    await callback.answer()

# ---------- Admin manage users ----------


@dp.message(Command("manage_users"))
async def manage_users(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.bot.send_message(
        message.from_user.id, "Управління учасниками:", reply_markup=admin_user_keyboard())


@dp.callback_query(F.data == "admin_add_user")
async def admin_add_user_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Немає доступу", show_alert=True)
        return
    await state.set_state(AdminAddUser.waiting_for_user)
    await callback.bot.send_message(
        callback.from_user.id,
        "Введіть дані у форматі:\n"
        "<code>user_id, first_name, last_name, username, gender</code>\n\n"
        "gender: <b>male</b> або <b>female</b>",
        parse_mode="HTML"
    )
    await callback.answer()


@dp.message(StateFilter(AdminAddUser.waiting_for_user))
async def admin_add_user_receive(message: Message, state: FSMContext):
    try:
        parts = [x.strip() for x in message.text.split(",")]
        user_id, first_name, last_name, username, gender = parts

        cursor.execute(
            "SELECT user_id FROM users WHERE user_id=?", (int(user_id),))
        if cursor.fetchone():
            await message.answer(
                f"⚠️ Користувач з ID {user_id} вже є в базі! Щоб оновити — спочатку видали його.")
            await state.clear()
            return

        cursor.execute("""
            INSERT INTO users(user_id, first_name, last_name, username, gender)
            VALUES (?, ?, ?, ?, ?)
        """, (int(user_id), first_name, last_name, username, gender))
        conn.commit()
        await message.answer(f"✅ Користувач {first_name} доданий!")
    except Exception as e:
        await message.answer(f"❌ Помилка: {e}\nПереконайся що формат правильний.")
    await state.clear()


@dp.callback_query(F.data == "admin_del_user")
async def admin_del_user_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Немає доступу", show_alert=True)
        return
    await state.set_state(AdminDeleteUser.waiting_for_id)
    await callback.bot.send_message(
        callback.from_user.id, "Введіть <b>user_id</b> для видалення:", parse_mode="HTML")
    await callback.answer()


@dp.message(StateFilter(AdminDeleteUser.waiting_for_id))
async def admin_del_user_receive(message: Message, state: FSMContext):
    if message.text.isdigit():
        cursor.execute("DELETE FROM users WHERE user_id=?",
                       (int(message.text),))
        conn.commit()
        await message.answer(f"✅ Користувач з ID {message.text} видалений")
    else:
        await message.answer("❌ Введіть числовий user_id")
    await state.clear()


@dp.callback_query(F.data == "admin_list_users")
async def admin_list_users(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Немає доступу", show_alert=True)
        return
    cursor.execute("SELECT user_id, first_name, gender FROM users")
    rows = cursor.fetchall()
    if not rows:
        await callback.bot.send_message(callback.from_user.id, "База користувачів порожня")
    else:
        text = "📋 Список учасників:\n"
        for uid, fname, gender in rows:
            label = "ч" if gender == "male" else "ж"
            text += f"{fname} ({label}) — ID: {uid}\n"
        await callback.bot.send_message(callback.from_user.id, text)
    await callback.answer()

# ---------- Main ----------


async def main():
    bot = Bot(token=TOKEN)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
