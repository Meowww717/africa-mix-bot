import asyncio
import os
import sqlite3
import random
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage


TOKEN = os.getenv("TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))

if not TOKEN:
    raise ValueError("TOKEN not found!")

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
    time TEXT
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


class LeaveConfirm(StatesGroup):
    choosing_leave_type = State()


class AdminAddUser(StatesGroup):
    waiting_for_user = State()


class AdminDeleteUser(StatesGroup):
    waiting_for_id = State()

# ---------- Helpers ----------


def get_meeting_id(message_id, chat_id):
    cursor.execute(
        "SELECT meeting_id FROM meetings WHERE message_id=? AND chat_id=?",
        (message_id, chat_id)
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


async def save_state_for_user(bot, user_id, meeting_id, display_name, user_gender, group_chat_id, group_message_id):
    """Зберігає стейт в контексті особистого чату юзера"""
    user_state = dp.fsm.get_context(bot=bot, chat_id=user_id, user_id=user_id)
    await user_state.update_data(
        meeting_id=meeting_id,
        user_id=user_id,
        display_name=display_name,
        user_gender=user_gender,
        chat_id=group_chat_id,
        message_id=group_message_id
    )
    await user_state.set_state(JoinMeeting.choosing_solo_or_pair)

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


def meeting_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏐 Записатись",
                              callback_data="join")],
        [InlineKeyboardButton(text="👫 Додати партнера",
                              callback_data="add_partner_from_base")],
        [InlineKeyboardButton(text="👤 Додати гостя",
                              callback_data="add_guest")],
        [InlineKeyboardButton(text="🔀 Розбити по парах",
                              callback_data="shuffle_pairs")],
        [InlineKeyboardButton(text="❌ Скасувати запис",
                              callback_data="leave")],
        [InlineKeyboardButton(text="🗑 Видалити зустріч",
                              callback_data="delete")],
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


def guest_solo_or_pair_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Самостійно", callback_data="guest_solo")],
        [InlineKeyboardButton(text="Парою",      callback_data="guest_pair")],
    ])


def gender_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Хлопець", callback_data="gender_male")],
        [InlineKeyboardButton(
            text="Дівчина",   callback_data="gender_female")],
    ])


def leave_type_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Тільки я",    callback_data="leave_solo")],
        [InlineKeyboardButton(text="Я і партнер", callback_data="leave_pair")],
    ])


def partners_keyboard(meeting_id, user_gender):
    needed_gender = opposite_gender(user_gender)

    # Люди з бази що ще не записались
    cursor.execute("""
        SELECT u.user_id, u.first_name
        FROM users u
        WHERE u.gender=?
        AND u.user_id NOT IN (
            SELECT user_id FROM participants WHERE meeting_id=?
        )
    """, (needed_gender, meeting_id))
    from_base = cursor.fetchall()

    # Вже записані одиночні учасники протилежної статі (включно з гостями)
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

# ---------- Create meeting ----------


@dp.message(Command("create"))
async def create_meeting(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    await message.delete()
    admin_state = dp.fsm.get_context(
        bot=message.bot, chat_id=ADMIN_ID, user_id=ADMIN_ID)
    await admin_state.update_data(chat_id=message.chat.id)
    await admin_state.set_state(CreateMeeting.choosing_day)
    await message.bot.send_message(ADMIN_ID, "📅 Обери день:", reply_markup=days_keyboard())


@dp.callback_query(F.data.startswith("day_"))
async def choose_day(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return
    day = callback.data.split("_", 1)[1]
    await state.update_data(day=day)
    await state.set_state(CreateMeeting.choosing_time)
    await callback.message.edit_text(f"📅 {day}\n\nТепер обери час:", reply_markup=time_keyboard())
    await callback.answer()


@dp.callback_query(F.data.startswith("time_"))
async def choose_time(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return
    data = await state.get_data()
    if "chat_id" not in data:
        await callback.answer("Помилка: почни знову через /create", show_alert=True)
        return
    time_val = callback.data.split("_", 1)[1]
    day = data["day"]
    chat_id = data["chat_id"]

    sent = await callback.bot.send_message(chat_id, "⏳ Завантаження...", reply_markup=meeting_keyboard())
    cursor.execute(
        "INSERT INTO meetings(message_id, chat_id, day, time) VALUES (?, ?, ?, ?)",
        (sent.message_id, chat_id, day, time_val)
    )
    conn.commit()
    meeting_id = cursor.lastrowid

    await sent.edit_text(format_text(meeting_id), reply_markup=meeting_keyboard())
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

    # Зберігаємо стейт для особистого чату юзера
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
        await callback.bot.send_message(user.id, "Як хочеш записатись?", reply_markup=solo_or_pair_keyboard())
    except Exception:
        await callback.answer("Спочатку напиши боту в особисті!", show_alert=True)
        await user_state.clear()
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
    await callback.message.edit_text("✅ Записано!")
    await state.clear()
    await callback.answer()


@dp.callback_query(StateFilter(JoinMeeting.choosing_solo_or_pair), F.data == "join_pair")
async def join_pair_choose(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    meeting_id = data["meeting_id"]
    user_gender = data["user_gender"]

    kb = partners_keyboard(meeting_id, user_gender)
    if not kb:
        await callback.answer("Немає доступних партнерів протилежної статі 😔", show_alert=True)
        return

    await state.set_state(JoinMeeting.choosing_partner)
    await callback.message.edit_text("Обери партнера/партнерку:", reply_markup=kb)
    await callback.answer()


@dp.callback_query(StateFilter(JoinMeeting.choosing_partner), F.data == "pick_partner_guest")
async def pick_partner_guest_ask(callback: CallbackQuery, state: FSMContext):
    await state.set_state(JoinMeeting.entering_manual_partner_name)
    await callback.message.edit_text(
        "Введи ім'я партнера/партнерки (буде автоматично іншої статі):"
    )
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
    await message.answer(f"✅ Записано пару: {display_name} / {guest_name}!")
    await state.clear()


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
    if not p_row:
        await callback.answer("Партнер не знайдений.", show_alert=True)
        return

    partner_name, partner_gender = p_row
    pid = next_pair_id(meeting_id)

    # Перевіряємо чи юзер вже є в учасниках (записаний одиночно)
    if is_registered(meeting_id, user_id):
        # Оновлюємо існуючий запис — додаємо pair_id
        cursor.execute(
            "UPDATE participants SET pair_id=? WHERE meeting_id=? AND user_id=?",
            (pid, meeting_id, user_id)
        )
    else:
        # Додаємо нового учасника з парою
        cursor.execute(
            "INSERT OR IGNORE INTO participants(meeting_id, user_id, display_name, pair_id, gender) VALUES (?, ?, ?, ?, ?)",
            (meeting_id, user_id, display_name, pid, user_gender)
        )

    # Партнера завжди додаємо або оновлюємо
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
    if not kb:
        await callback.answer("Немає доступних партнерів протилежної статі 😔", show_alert=True)
        return

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
        await callback.answer("Спочатку напиши боту в особисті!", show_alert=True)
        await user_state.clear()
        return
    await callback.answer()

# ---------- Add guest ----------


@dp.callback_query(F.data == "add_guest")
async def add_guest_start(callback: CallbackQuery, state: FSMContext):
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
        message_id=callback.message.message_id,
        chat_id=callback.message.chat.id
    )
    await user_state.set_state(AddGuest.entering_name)

    try:
        await callback.bot.send_message(user_id, "👤 Введи ім'я гостя:")
    except Exception:
        await callback.answer("Спочатку напиши боту в особисті!", show_alert=True)
        await user_state.clear()
        return
    await callback.answer()


@dp.message(StateFilter(AddGuest.entering_name))
async def add_guest_name(message: Message, state: FSMContext):
    await state.update_data(guest_name=message.text.strip())
    await state.set_state(AddGuest.choosing_solo_or_pair)
    await message.answer("Як додати гостя?", reply_markup=guest_solo_or_pair_keyboard())


@dp.callback_query(StateFilter(AddGuest.choosing_solo_or_pair), F.data == "guest_solo")
async def add_guest_solo_gender(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AddGuest.choosing_guest_gender)
    await callback.message.edit_text("Стать гостя?", reply_markup=gender_keyboard())
    await callback.answer()


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
        await callback.message.edit_text(f"✅ Додано гостя: {guest_name}!")

    await state.clear()
    await callback.answer()


@dp.callback_query(StateFilter(AddGuest.choosing_solo_or_pair), F.data == "guest_pair")
async def add_guest_pair_ask(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AddGuest.entering_partner_name)
    await callback.message.edit_text("Введи ім'я партнера гостя:")
    await callback.answer()


@dp.message(StateFilter(AddGuest.entering_partner_name))
async def add_guest_pair_partner_name(message: Message, state: FSMContext):
    await state.update_data(partner_name=message.text.strip())
    await state.set_state(AddGuest.choosing_guest_gender)
    data = await state.get_data()
    await message.answer(f"Стать для {data['guest_name']}?", reply_markup=gender_keyboard())

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
            await callback.bot.send_message(user_id, "Ти в парі. Що скасувати?", reply_markup=leave_type_keyboard())
        except Exception:
            await callback.answer("Спочатку напиши боту в особисті!", show_alert=True)
            await user_state.clear()
            return
    else:
        cursor.execute(
            "DELETE FROM participants WHERE meeting_id=? AND user_id=?", (meeting_id, user_id))
        conn.commit()
        await callback.message.edit_text(format_text(meeting_id), reply_markup=meeting_keyboard())
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
    await callback.message.edit_text("✅ Пару скасовано повністю.")
    await state.clear()
    await callback.answer()

# ---------- Shuffle pairs ----------


@dp.callback_query(F.data == "shuffle_pairs")
async def shuffle_pairs(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Тільки адмін може розбивати по парах", show_alert=True)
        return

    meeting_id = get_meeting_id(
        callback.message.message_id, callback.message.chat.id)
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

    random.shuffle(males)
    random.shuffle(females)

    paired = 0
    while males and females:
        m = males.pop()
        f = females.pop()
        pid = next_pair_id(meeting_id)
        cursor.execute(
            "UPDATE participants SET pair_id=? WHERE meeting_id=? AND user_id=?", (pid, meeting_id, m[0]))
        cursor.execute(
            "UPDATE participants SET pair_id=? WHERE meeting_id=? AND user_id=?", (pid, meeting_id, f[0]))
        paired += 1

    conn.commit()
    await callback.message.edit_text(format_text(meeting_id), reply_markup=meeting_keyboard())
    await callback.answer(f"✅ Створено {paired} пар!")

# ---------- Delete meeting ----------


@dp.callback_query(F.data == "delete")
async def delete_meeting(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Тільки адмін може видаляти", show_alert=True)
        return
    meeting_id = get_meeting_id(
        callback.message.message_id, callback.message.chat.id)
    if meeting_id:
        cursor.execute(
            "DELETE FROM meetings WHERE meeting_id=?", (meeting_id,))
        cursor.execute(
            "DELETE FROM participants WHERE meeting_id=?", (meeting_id,))
        conn.commit()
    await callback.message.delete()
    await callback.answer()

# ---------- Admin manage users ----------


@dp.message(Command("manage_users"))
async def manage_users(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    await message.bot.send_message(ADMIN_ID, "Управління учасниками:", reply_markup=admin_user_keyboard())


@dp.callback_query(F.data == "admin_add_user")
async def admin_add_user_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Немає доступу", show_alert=True)
        return
    await state.set_state(AdminAddUser.waiting_for_user)
    await callback.bot.send_message(
        ADMIN_ID,
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
            await message.bot.send_message(ADMIN_ID, f"⚠️ Користувач з ID {user_id} вже є в базі! Щоб оновити — спочатку видали його.")
            await state.clear()
            return

        cursor.execute("""
            INSERT INTO users(user_id, first_name, last_name, username, gender)
            VALUES (?, ?, ?, ?, ?)
        """, (int(user_id), first_name, last_name, username, gender))
        conn.commit()
        await message.bot.send_message(ADMIN_ID, f"✅ Користувач {first_name} доданий!")
    except Exception as e:
        await message.bot.send_message(ADMIN_ID, f"❌ Помилка: {e}\nПереконайся що формат правильний.")
    await state.clear()


@dp.callback_query(F.data == "admin_del_user")
async def admin_del_user_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Немає доступу", show_alert=True)
        return
    await state.set_state(AdminDeleteUser.waiting_for_id)
    await callback.bot.send_message(ADMIN_ID, "Введіть <b>user_id</b> для видалення:", parse_mode="HTML")
    await callback.answer()


@dp.message(StateFilter(AdminDeleteUser.waiting_for_id))
async def admin_del_user_receive(message: Message, state: FSMContext):
    if message.text.isdigit():
        cursor.execute("DELETE FROM users WHERE user_id=?",
                       (int(message.text),))
        conn.commit()
        await message.bot.send_message(ADMIN_ID, f"✅ Користувач з ID {message.text} видалений")
    else:
        await message.bot.send_message(ADMIN_ID, "❌ Введіть числовий user_id")
    await state.clear()


@dp.callback_query(F.data == "admin_list_users")
async def admin_list_users(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Немає доступу", show_alert=True)
        return
    cursor.execute("SELECT user_id, first_name, gender FROM users")
    rows = cursor.fetchall()
    if not rows:
        await callback.bot.send_message(ADMIN_ID, "База користувачів порожня")
    else:
        text = "📋 Список учасників:\n"
        for uid, fname, gender in rows:
            label = "ч" if gender == "male" else "ж"
            text += f"{fname} ({label}) — ID: {uid}\n"
        await callback.bot.send_message(ADMIN_ID, text)
    await callback.answer()

# ---------- Main ----------


async def main():
    bot = Bot(token=TOKEN)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
