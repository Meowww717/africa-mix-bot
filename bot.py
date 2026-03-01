import asyncio
import os
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery
)
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage

TOKEN = os.getenv("TOKEN")
ADMIN_ID = 561261084

if not TOKEN:
    raise ValueError("TOKEN not found!")

storage = MemoryStorage()
dp = Dispatcher(storage=storage)

polls = {}


class CreateMeeting(StatesGroup):
    choosing_day = State()
    choosing_time = State()


class AddPartner(StatesGroup):
    waiting_for_name = State()


# ---------- Клавіатури ----------

def days_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="Понеділок", callback_data="day_Понеділок")],
        [InlineKeyboardButton(text="Середа", callback_data="day_Середа")],
        [InlineKeyboardButton(text="Пʼятниця", callback_data="day_Пʼятниця")],
        [InlineKeyboardButton(text="Неділя", callback_data="day_Неділя")]
    ])


def time_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="18:00", callback_data="time_18:00")],
        [InlineKeyboardButton(text="19:00", callback_data="time_19:00")],
        [InlineKeyboardButton(text="20:00", callback_data="time_20:00")]
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


# ---------- Форматування ----------

def format_text(day, time, participants):
    text = (
        f"🌍🔥 Африканці, граємо?\n\n"
        f"⚽ Діставайте кеди — буде спекотно!\n"
        f"📅 {day}\n"
        f"🕖 {time}\n"
        f"📍 Трек\n\n"
        f"👥 Гравці:\n"
    )

    if not participants:
        text += "Поки що тиша... хто перший? 😏"
    else:
        for i, p in enumerate(participants, 1):
            text += f"{i}. {p}\n"

    return text


# ---------- Створення ----------

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

    await callback.message.edit_text(
        f"📅 {day}\n\nТепер обери час 🕖",
        reply_markup=time_keyboard()
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("time_"))
async def choose_time(callback: CallbackQuery, state: FSMContext):
    time = callback.data.split("_")[1]
    data = await state.get_data()
    day = data["day"]

    text = format_text(day, time, [])

    await callback.message.edit_text(
        text,
        reply_markup=meeting_keyboard()
    )

    polls[callback.message.message_id] = {
        "day": day,
        "time": time,
        "participants": []
    }

    await state.clear()
    await callback.answer()


# ---------- Запис ----------

@dp.callback_query(F.data == "join")
async def join(callback: CallbackQuery):
    poll = polls.get(callback.message.message_id)
    if not poll:
        return

    name = callback.from_user.first_name

    if name not in poll["participants"]:
        poll["participants"].append(name)

    new_text = format_text(poll["day"], poll["time"], poll["participants"])
    await callback.message.edit_text(new_text, reply_markup=meeting_keyboard())
    await callback.answer()


# ---------- Скасування запису ----------

@dp.callback_query(F.data == "leave")
async def leave(callback: CallbackQuery):
    poll = polls.get(callback.message.message_id)
    if not poll:
        return

    name = callback.from_user.first_name
    poll["participants"] = [
        p for p in poll["participants"] if not p.startswith(name)]

    new_text = format_text(poll["day"], poll["time"], poll["participants"])
    await callback.message.edit_text(new_text, reply_markup=meeting_keyboard())
    await callback.answer()


# ---------- Додати пару ----------

@dp.callback_query(F.data == "add_partner")
async def add_partner(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AddPartner.waiting_for_name)
    await state.update_data(message_id=callback.message.message_id)
    await callback.answer("Введи ім’я партнера")


@dp.message(AddPartner.waiting_for_name)
async def process_partner(message: Message, state: FSMContext):
    data = await state.get_data()
    message_id = data["message_id"]

    poll = polls.get(message_id)
    if not poll:
        return

    user_name = message.from_user.first_name
    partner_name = message.text.strip()

    updated = False
    for i, p in enumerate(poll["participants"]):
        if p.startswith(user_name):
            poll["participants"][i] = f"{user_name} + {partner_name}"
            updated = True
            break

    if not updated:
        poll["participants"].append(f"{user_name} + {partner_name}")

    new_text = format_text(poll["day"], poll["time"], poll["participants"])

    await message.bot.edit_message_text(
        chat_id=message.chat.id,
        message_id=message_id,
        text=new_text,
        reply_markup=meeting_keyboard()
    )

    await state.clear()


# ---------- Видалення зустрічі (тільки адмін) ----------

@dp.callback_query(F.data == "delete")
async def delete_meeting(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Тільки адмін може видаляти", show_alert=True)
        return

    if callback.message.message_id in polls:
        del polls[callback.message.message_id]

    await callback.message.delete()
    await callback.answer()


# ---------- MAIN ----------

async def main():
    bot = Bot(token=TOKEN)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
