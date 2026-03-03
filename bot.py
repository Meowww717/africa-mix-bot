import asyncio
import os
import sqlite3
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.filters import Command
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
conn = sqlite3.connect("bot.db")
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
    entering_partner_name = State()


class AddGuest(StatesGroup):
    entering_guest_name = State()


class AddPartner(StatesGroup):
    entering_partner_name = State()


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
        "SELECT id FROM participants WHERE meeting_id=? AND user_id=?",
        (meeting_id, user_id)
    )
    return cursor.fetchone() is not None


def next_guest_id(meeting_id):
    cursor.execute(
        "SELECT MIN(user_id) FROM participants WHERE meeting_id=? AND user_id < 0",
        (meeting_id,)
    )
    row = cursor.fetchone()
    return (row[0] or 0) - 1

# ---------- Keyboards ----------


def days_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="Понеділок", callback_data="day_Понеділок")],
        [InlineKeyboardButton(text="Середа",    callback_data="day_Середа")],
        [InlineKeyboardButton(text="Пятниця",   callback_data="day_Пятниця")],
        [InlineKeyboardButton(text="Неділя",    callback_data="day_Неділя")],
    ])


def time_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="18:00", callback_data="time_18:00")],
        [InlineKeyboardButton(text="19:00", callback_data="time_19:00")],
        [InlineKeyboardButton(text="20:00", callback_data="time_20:00")],
    ])


def meeting_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚽ Записатись",
                              callback_data="join")],
        [InlineKeyboardButton(text="👫 Додати партнера",
                              callback_data="add_partner")],
        [InlineKeyboardButton(text="👤 Додати гостя",
                              callback_data="add_guest")],
        [InlineKeyboardButton(text="❌ Скасувати запис",
                              callback_data="leave")],
        [InlineKeyboardButton(text="🗑 Видалити зустріч",
                              callback_data="delete")],
    ])


def solo_or_pair_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🙋 Сам(а)",  callback_data="join_solo")],
        [InlineKeyboardButton(text="👫 З парою", callback_data="join_pair")],
    ])


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
        SELECT p.user_id, p.display_name, p.pair_id, u.gender
        FROM participants p
        LEFT JOIN users u ON u.user_id = p.user_id
        WHERE p.meeting_id=?
        ORDER BY p.pair_id, p.id
    """, (meeting_id,))
    rows = cursor.fetchall()

    text = f"🌍🔥 Африканці, граємо?\n\n📅 {day}\n🕖 {time}\n\n👥 Гравці:\n"

    if not rows:
        text += "Поки що тиша... хто перший? 😏"
        return text

    pairs = {}
    singles = []
    for user_id, display_name, pair_id, gender in rows:
        icon = "♀️" if gender == "female" else "♂️"
        label = f"{display_name} {icon}"
        if pair_id:
            pairs.setdefault(pair_id, []).append(label)
        else:
            singles.append(label)

    counter = 1
    for pid, names in pairs.items():
        if len(names) >= 2:
            text += f"{counter}. 👫 {names[0]} + {names[1]}\n"
        else:
            text += f"{counter}. 🙋 {names[0]} (шукає пару)\n"
        counter += 1

    for name in singles:
        text += f"{counter}. 🙋 {name}\n"
        counter += 1

    return text

# ---------- Create meeting ----------


@dp.message(Command("create"))
async def create_meeting(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    await message.delete()
    await state.update_data(chat_id=message.chat.id)
    await state.set_state(CreateMeeting.choosing_day)
    await message.bot.send_message(ADMIN_ID, "📅 Обери день:", reply_markup=days_keyboard())


@dp.callback_query(F.data.startswith("day_"), CreateMeeting.choosing_day)
async def choose_day(callback: CallbackQuery, state: FSMContext):
    day = callback.data.split("_", 1)[1]
    await state.update_data(day=day)
    await state.set_state(CreateMeeting.choosing_time)
    await callback.message.edit_text(f"📅 {day}\n\nТепер обери час 🕖", reply_markup=time_keyboard())
    await callback.answer()


@dp.callback_query(F.data.startswith("time_"), CreateMeeting.choosing_time)
async def choose_time(callback: CallbackQuery, state: FSMContext):
    time_val = callback.data.split("_", 1)[1]
    data = await state.get_data()
    day = data["day"]
    chat_id = data["chat_id"]

    sent = await callback.bot.send_message(
        chat_id,
        "⏳ Завантаження...",
        reply_markup=meeting_keyboard()
    )

    cursor.execute(
        "INSERT INTO meetings(message_id, chat_id, day, time) VALUES (?, ?, ?, ?)",
        (sent.message_id, chat_id, day, time_val)
    )
    conn.commit()
    meeting_id = cursor.lastrowid

    text = format_text(meeting_id)
    await sent.edit_text(text, reply_markup=meeting_keyboard())
    await callback.message.edit_text("✅ Зустріч створена!")
    await state.clear()
    await callback.answer()

# ---------- Join ----------


@dp.callback_query(F.data == "join")
async def join_start(callback: CallbackQuery, state: FSMContext):
    user = callback.from_user
    cursor.execute("SELECT first_name FROM users WHERE user_id=?", (user.id,))
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

    await state.update_data(
        meeting_id=meeting_id,
        user_id=user.id,
        display_name=row[0],
        message_id=callback.message.message_id,
        chat_id=callback.message.chat.id
    )
    await state.set_state(JoinMeeting.choosing_solo_or_pair)
    await callback.message.answer("Як хочеш записатись?", reply_markup=solo_or_pair_keyboard())
    await callback.answer()


@dp.callback_query(F.data == "join_solo", JoinMeeting.choosing_solo_or_pair)
async def join_solo(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    meeting_id = data["meeting_id"]

    cursor.execute(
        "INSERT OR IGNORE INTO participants(meeting_id, user_id, display_name, pair_id) VALUES (?, ?, ?, NULL)",
        (meeting_id, data["user_id"], data["display_name"])
    )
    conn.commit()

    text = format_text(meeting_id)
    await callback.message.delete()
    await callback.bot.edit_message_text(
        text,
        chat_id=data["chat_id"],
        message_id=data["message_id"],
        reply_markup=meeting_keyboard()
    )
    await state.clear()
    await callback.answer("✅ Записано!")


@dp.callback_query(F.data == "join_pair", JoinMeeting.choosing_solo_or_pair)
async def join_pair_ask(callback: CallbackQuery, state: FSMContext):
    await state.set_state(JoinMeeting.entering_partner_name)
    await callback.message.edit_text("👫 Введи ім'я свого партнера/партнерки:")
    await callback.answer()


@dp.message(JoinMeeting.entering_partner_name)
async def join_pair_save(message: Message, state: FSMContext):
    partner_name = message.text.strip()
    data = await state.get_data()
    meeting_id = data["meeting_id"]
    user_id = data["user_id"]
    display_name = data["display_name"]

    pid = next_pair_id(meeting_id)
    guest_id = next_guest_id(meeting_id)

    cursor.execute(
        "INSERT OR IGNORE INTO participants(meeting_id, user_id, display_name, pair_id) VALUES (?, ?, ?, ?)",
        (meeting_id, user_id, display_name, pid)
    )
    cursor.execute(
        "INSERT OR IGNORE INTO participants(meeting_id, user_id, display_name, pair_id) VALUES (?, ?, ?, ?)",
        (meeting_id, guest_id, partner_name, pid)
    )
    conn.commit()

    text = format_text(meeting_id)
    await message.bot.edit_message_text(
        text,
        chat_id=data["chat_id"],
        message_id=data["message_id"],
        reply_markup=meeting_keyboard()
    )
    await message.answer(f"✅ Записано пару: {display_name} + {partner_name}!")
    await state.clear()

# ---------- Add partner (для вже записаних одиночно) ----------


@dp.callback_query(F.data == "add_partner")
async def add_partner_start(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    meeting_id = get_meeting_id(
        callback.message.message_id, callback.message.chat.id)
    if not meeting_id:
        await callback.answer("Зустріч не знайдена.", show_alert=True)
        return
    if not is_registered(meeting_id, user_id):
        await callback.answer("Спочатку запишись на зустріч ⚽", show_alert=True)
        return

    # Перевіряємо чи вже є партнер
    cursor.execute(
        "SELECT pair_id FROM participants WHERE meeting_id=? AND user_id=?",
        (meeting_id, user_id)
    )
    row = cursor.fetchone()
    if row and row[0]:
        await callback.answer("Ти вже в парі! Спочатку скасуй запис ❌", show_alert=True)
        return

    await state.update_data(
        meeting_id=meeting_id,
        user_id=user_id,
        message_id=callback.message.message_id,
        chat_id=callback.message.chat.id
    )
    await state.set_state(AddPartner.entering_partner_name)
    await callback.message.answer("👫 Введи ім'я свого партнера/партнерки:")
    await callback.answer()


@dp.message(AddPartner.entering_partner_name)
async def add_partner_save(message: Message, state: FSMContext):
    partner_name = message.text.strip()
    data = await state.get_data()
    meeting_id = data["meeting_id"]
    user_id = data["user_id"]

    pid = next_pair_id(meeting_id)
    guest_id = next_guest_id(meeting_id)

    # Оновлюємо існуючий запис — додаємо pair_id
    cursor.execute(
        "UPDATE participants SET pair_id=? WHERE meeting_id=? AND user_id=?",
        (pid, meeting_id, user_id)
    )
    # Додаємо партнера як гостя
    cursor.execute(
        "INSERT OR IGNORE INTO participants(meeting_id, user_id, display_name, pair_id) VALUES (?, ?, ?, ?)",
        (meeting_id, guest_id, partner_name, pid)
    )
    conn.commit()

    text = format_text(meeting_id)
    await message.bot.edit_message_text(
        text,
        chat_id=data["chat_id"],
        message_id=data["message_id"],
        reply_markup=meeting_keyboard()
    )
    await message.answer(f"✅ Партнер {partner_name} доданий!")
    await state.clear()

# ---------- Add guest ----------


@dp.callback_query(F.data == "add_guest")
async def add_guest_start(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    meeting_id = get_meeting_id(
        callback.message.message_id, callback.message.chat.id)
    if not meeting_id:
        await callback.answer("Зустріч не знайдена.", show_alert=True)
        return
    if not is_registered(meeting_id, user_id):
        await callback.answer("Лише записані гравці можуть додавати гостей.", show_alert=True)
        return

    await state.update_data(
        meeting_id=meeting_id,
        message_id=callback.message.message_id,
        chat_id=callback.message.chat.id
    )
    await state.set_state(AddGuest.entering_guest_name)
    await callback.message.answer(
        "👤 Введи ім'я гостя.\n"
        "Щоб додати пару — напиши через '+' імена\n",
        parse_mode="HTML"
    )
    await callback.answer()


@dp.message(AddGuest.entering_guest_name)
async def add_guest_save(message: Message, state: FSMContext):
    text_input = message.text.strip()
    data = await state.get_data()
    meeting_id = data["meeting_id"]

    if "+" in text_input:
        parts = [p.strip() for p in text_input.split("+", 1)]
        name1, name2 = parts[0], parts[1]
        pid = next_pair_id(meeting_id)
        id1 = next_guest_id(meeting_id)
        cursor.execute(
            "INSERT INTO participants(meeting_id, user_id, display_name, pair_id) VALUES (?, ?, ?, ?)",
            (meeting_id, id1, name1, pid)
        )
        id2 = next_guest_id(meeting_id)
        cursor.execute(
            "INSERT INTO participants(meeting_id, user_id, display_name, pair_id) VALUES (?, ?, ?, ?)",
            (meeting_id, id2, name2, pid)
        )
        reply = f"✅ Додано пару: {name1} + {name2}!"
    else:
        fake_id = next_guest_id(meeting_id)
        cursor.execute(
            "INSERT INTO participants(meeting_id, user_id, display_name, pair_id) VALUES (?, ?, ?, NULL)",
            (meeting_id, fake_id, text_input)
        )
        reply = f"✅ Додано гостя: {text_input}!"

    conn.commit()
    fmt = format_text(meeting_id)
    await message.bot.edit_message_text(
        fmt,
        chat_id=data["chat_id"],
        message_id=data["message_id"],
        reply_markup=meeting_keyboard()
    )
    await message.answer(reply)
    await state.clear()

# ---------- Leave ----------


@dp.callback_query(F.data == "leave")
async def leave(callback: CallbackQuery):
    user_id = callback.from_user.id
    meeting_id = get_meeting_id(
        callback.message.message_id, callback.message.chat.id)
    if not meeting_id:
        await callback.answer("Зустріч не знайдена.", show_alert=True)
        return

    cursor.execute(
        "SELECT pair_id FROM participants WHERE meeting_id=? AND user_id=?",
        (meeting_id, user_id)
    )
    row = cursor.fetchone()
    if not row:
        await callback.answer("Тебе немає в списку.", show_alert=True)
        return

    pair_id = row[0]
    if pair_id:
        cursor.execute(
            "DELETE FROM participants WHERE meeting_id=? AND pair_id=?",
            (meeting_id, pair_id)
        )
    else:
        cursor.execute(
            "DELETE FROM participants WHERE meeting_id=? AND user_id=?",
            (meeting_id, user_id)
        )
    conn.commit()

    text = format_text(meeting_id)
    await callback.message.edit_text(text, reply_markup=meeting_keyboard())
    await callback.answer("✅ Запис скасовано")

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


@dp.message(AdminAddUser.waiting_for_user)
async def admin_add_user_receive(message: Message, state: FSMContext):
    try:
        parts = [x.strip() for x in message.text.split(",")]
        user_id, first_name, last_name, username, gender = parts
        cursor.execute("""
            INSERT OR REPLACE INTO users(user_id, first_name, last_name, username, gender)
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


@dp.message(AdminDeleteUser.waiting_for_id)
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
            icon = "♀️" if gender == "female" else "♂️"
            text += f"{fname} {icon} — ID: {uid}\n"
        await callback.bot.send_message(ADMIN_ID, text)
    await callback.answer()

# ---------- Main ----------


async def main():
    bot = Bot(token=TOKEN)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
