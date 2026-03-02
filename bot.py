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
ADMIN_ID = 561261084  # твій Telegram ID

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
    message_id INTEGER PRIMARY KEY,
    day TEXT,
    time TEXT
)
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS participants (
    meeting_id INTEGER,
    user_id INTEGER,
    partner_id INTEGER,
    PRIMARY KEY(meeting_id, user_id)
)
""")
conn.commit()

# ---------- FSM ----------


class CreateMeeting(StatesGroup):
    choosing_day = State()
    choosing_time = State()


class AddPartner(StatesGroup):
    choosing_partner = State()


class AdminAddUser(StatesGroup):
    waiting_for_user = State()

# ---------- Keyboards ----------


def days_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="Понеділок", callback_data="day_Понеділок")],
        [InlineKeyboardButton(text="Середа", callback_data="day_Середа")],
        [InlineKeyboardButton(text="Пʼятниця", callback_data="day_Пʼятниця")],
        [InlineKeyboardButton(text="Неділя", callback_data="day_Неділя")],
    ])


def time_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="18:00", callback_data="time_18:00")],
        [InlineKeyboardButton(text="19:00", callback_data="time_19:00")],
        [InlineKeyboardButton(text="20:00", callback_data="time_20:00")],
    ])


def meeting_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚽ Записатись", callback_data="join")],
        [InlineKeyboardButton(text="➕ Додати пару",
                              callback_data="add_partner")],
        [InlineKeyboardButton(text="❌ Скасувати запис",
                              callback_data="leave")],
        [InlineKeyboardButton(text="🗑 Видалити зустріч",
                              callback_data="delete")]
    ])


def admin_user_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Додати учасника",
                              callback_data="admin_add_user")],
        [InlineKeyboardButton(text="❌ Видалити учасника",
                              callback_data="admin_del_user")],
        [InlineKeyboardButton(text="📋 Показати всіх учасників",
                              callback_data="admin_list_users")]
    ])

# ---------- Format text ----------


def format_text(meeting_id):
    cursor.execute(
        "SELECT day, time FROM meetings WHERE message_id=?", (meeting_id,))
    meeting = cursor.fetchone()
    if not meeting:
        return "Зустріч скасована"
    day, time = meeting
    text = f"🌍🔥 Африканці, граємо?\n\n📅 {day}\n🕖 {time}\n👥 Гравці:\n"
    cursor.execute("""
        SELECT u.first_name, u.gender, p.partner_id
        FROM participants p
        JOIN users u ON u.user_id = p.user_id
        WHERE p.meeting_id=?
    """, (meeting_id,))
    rows = cursor.fetchall()
    if not rows:
        text += "Поки що тиша... хто перший? 😏"
    else:
        for i, row in enumerate(rows, 1):
            name, gender, partner_id = row
            icon = "💃" if gender == "female" else "🕺"
            if partner_id:
                cursor.execute(
                    "SELECT first_name, gender FROM users WHERE user_id=?", (partner_id,))
                p_row = cursor.fetchone()
                if p_row:
                    p_name, p_gender = p_row
                    p_icon = "👩" if p_gender == "female" else "👨"
                    text += f"{i}. {name} {icon} + {p_name} {p_icon}\n"
            else:
                text += f"{i}. {name} {icon}\n"
    return text

# ---------- User management ----------


def add_user(user_id, first_name, last_name, username, gender):
    cursor.execute("""
        INSERT OR REPLACE INTO users(user_id, first_name, last_name, username, gender)
        VALUES (?, ?, ?, ?, ?)
    """, (user_id, first_name, last_name, username, gender))
    conn.commit()


def remove_user(user_id):
    cursor.execute("DELETE FROM users WHERE user_id=?", (user_id,))
    conn.commit()

# ---------- Handlers ----------

# Створення зустрічі


@dp.message(Command("create"))
async def create_meeting(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    await state.set_state(CreateMeeting.choosing_day)
    await message.answer("📅 Обери день:", reply_markup=days_keyboard())


@dp.callback_query(F.data.startswith("day_"))
async def choose_day(callback: CallbackQuery, state: FSMContext):
    day = callback.data.split("_")[1]
    await state.update_data(day=day)
    await state.set_state(CreateMeeting.choosing_time)
    await callback.message.edit_text(f"📅 {day}\n\nТепер обери час 🕖", reply_markup=time_keyboard())
    await callback.answer()


@dp.callback_query(F.data.startswith("time_"))
async def choose_time(callback: CallbackQuery, state: FSMContext):
    time = callback.data.split("_")[1]
    data = await state.get_data()
    day = data["day"]
    cursor.execute("INSERT INTO meetings(message_id, day, time) VALUES (?, ?, ?)",
                   (callback.message.message_id, day, time))
    conn.commit()
    text = format_text(callback.message.message_id)
    await callback.message.edit_text(text, reply_markup=meeting_keyboard())
    await state.clear()
    await callback.answer()

# Запис на тренування


@dp.callback_query(F.data == "join")
async def join(callback: CallbackQuery):
    user = callback.from_user
    cursor.execute("SELECT * FROM users WHERE user_id=?", (user.id,))
    if not cursor.fetchone():
        await callback.answer("Тебе немає в базі користувачів. Адмін має додати.", show_alert=True)
        return
    cursor.execute("INSERT OR IGNORE INTO participants(meeting_id, user_id, partner_id) VALUES (?, ?, ?)",
                   (callback.message.message_id, user.id, None))
    conn.commit()
    text = format_text(callback.message.message_id)
    await callback.message.edit_text(text, reply_markup=meeting_keyboard())
    await callback.answer()

# Скасувати запис


@dp.callback_query(F.data == "leave")
async def leave(callback: CallbackQuery):
    user_id = callback.from_user.id
    meeting_id = callback.message.message_id
    cursor.execute(
        "DELETE FROM participants WHERE meeting_id=? AND user_id=?", (meeting_id, user_id))
    cursor.execute(
        "DELETE FROM participants WHERE meeting_id=? AND partner_id=?", (meeting_id, user_id))
    conn.commit()
    text = format_text(meeting_id)
    await callback.message.edit_text(text, reply_markup=meeting_keyboard())
    await callback.answer()

# Видалити зустріч


@dp.callback_query(F.data == "delete")
async def delete_meeting(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Тільки адмін може видаляти", show_alert=True)
        return
    meeting_id = callback.message.message_id
    cursor.execute("DELETE FROM meetings WHERE message_id=?", (meeting_id,))
    cursor.execute(
        "DELETE FROM participants WHERE meeting_id=?", (meeting_id,))
    conn.commit()
    await callback.message.delete()
    await callback.answer()

# Додати пару


@dp.callback_query(F.data == "add_partner")
async def add_partner(callback: CallbackQuery, state: FSMContext):
    meeting_id = callback.message.message_id
    cursor.execute(
        "SELECT user_id, first_name FROM users WHERE user_id != ?", (callback.from_user.id,))
    rows = cursor.fetchall()
    buttons = [[InlineKeyboardButton(
        row[1], callback_data=f"partner_{row[0]}")] for row in rows]
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    await state.update_data(meeting_id=meeting_id, user_id=callback.from_user.id)
    await state.set_state(AddPartner.choosing_partner)
    await callback.message.answer("Оберіть партнера:", reply_markup=kb)
    await callback.answer()


@dp.callback_query(F.data.startswith("partner_"))
async def process_partner(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    meeting_id = data["meeting_id"]
    user_id = data["user_id"]
    partner_id = int(callback.data.split("_")[1])
    cursor.execute("UPDATE participants SET partner_id=? WHERE meeting_id=? AND user_id=?",
                   (partner_id, meeting_id, user_id))
    conn.commit()
    text = format_text(meeting_id)
    await callback.message.edit_text(text, reply_markup=meeting_keyboard())
    await state.clear()
    await callback.answer()

# ---------- Admin manage users ----------


@dp.message(Command("manage_users"))
async def manage_users(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    await message.answer("Управління учасниками:", reply_markup=admin_user_keyboard())

# Додавання користувача вручну


@dp.callback_query(F.data == "admin_add_user")
async def admin_add_user_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminAddUser.waiting_for_user)
    await callback.message.answer(
        "Введіть дані користувача у форматі:\nuser_id, first_name, last_name, username, gender"
    )
    await callback.answer()


@dp.message(AdminAddUser.waiting_for_user)
async def admin_add_user_receive(message: Message, state: FSMContext):
    try:
        user_id, first_name, last_name, username, gender = [
            x.strip() for x in message.text.split(",")]
        add_user(int(user_id), first_name, last_name, username, gender)
        await message.answer(f"✅ Користувач {first_name} доданий в базу!")
    except Exception as e:
        await message.answer(f"❌ Помилка: {e}\nПереконайтесь, що формат правильний.")
    await state.clear()

# Видалення користувача


@dp.callback_query(F.data == "admin_del_user")
async def admin_del_user(callback: CallbackQuery):
    await callback.message.answer("Щоб видалити користувача вручну, введіть: user_id")
    await callback.answer()


@dp.message()
async def admin_del_user_receive(message: Message):
    if message.text.isdigit():
        remove_user(int(message.text))
        await message.answer(f"✅ Користувач з ID {message.text} видалений з бази")
    else:
        await message.answer("❌ Введіть числовий user_id")

# Показати всіх користувачів


@dp.callback_query(F.data == "admin_list_users")
async def admin_list_users(callback: CallbackQuery):
    cursor.execute("SELECT user_id, first_name, gender FROM users")
    rows = cursor.fetchall()
    if not rows:
        await callback.message.answer("База користувачів порожня")
        await callback.answer()
        return
    text = "📋 Список учасників:\n"
    for row in rows:
        icon = "👩" if row[2] == "female" else "👨"
        text += f"{row[1]} {icon} — ID: {row[0]}\n"
    await callback.message.answer(text)
    await callback.answer()

# ---------- Main ----------


async def main():
    bot = Bot(token=TOKEN)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
