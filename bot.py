import asyncio
import os
from aiohttp import web
from aiogram import Bot, Dispatcher
from aiogram.types import (
    Message,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    CallbackQuery
)
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder

# =====================================================
# НАЛАШТУВАННЯ
# =====================================================

TOKEN = os.getenv("TOKEN")
ADMIN_ID = 561261084

if not TOKEN:
    raise RuntimeError("TOKEN is not set. Use: fly secrets set TOKEN=...")

# =====================================================
# СТАН
# =====================================================

meetings = {}      # {message_id: {"participants": {user_id: [pairs]}}}
edit_state = {}    # {admin_id: message_id}
pair_state = {}    # {user_id: message_id}

# =====================================================
# HEALTHCHECK СЕРВЕР ДЛЯ FLY
# =====================================================


async def healthcheck(request):
    return web.Response(text="OK")


async def start_health_server():
    app = web.Application()
    app.router.add_get("/", healthcheck)

    runner = web.AppRunner(app)
    await runner.setup()

    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

# =====================================================
# ГОЛОВНА ЛОГІКА БОТА
# =====================================================


async def main():
    bot = Bot(token=TOKEN, parse_mode="HTML")
    dp = Dispatcher()

    # ---------------------------
    # Перевірка
    # ---------------------------
    @dp.message(Command("ping"))
    async def ping(message: Message):
        if message.from_user.id == ADMIN_ID:
            await message.answer("Бот працює ✅")

    # ---------------------------
    # Створення зустрічі
    # ---------------------------
    @dp.message(Command("meeting"))
    async def create_meeting(message: Message):
        if message.from_user.id != ADMIN_ID:
            return

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Записатись", callback_data="join")],
            [InlineKeyboardButton(text="Додати пару",
                                  callback_data="add_pair")],
            [InlineKeyboardButton(text="Редагувати", callback_data="edit")],
            [InlineKeyboardButton(text="Видалити", callback_data="delete")]
        ])

        msg = await message.answer(
            "🕒 <b>Нова зустріч</b>\nЧас: 15:00\n\nНатисніть кнопку нижче:",
            reply_markup=keyboard
        )

        meetings[msg.message_id] = {"participants": {}}

    # ---------------------------
    # Обробка кнопок
    # ---------------------------
    @dp.callback_query()
    async def callbacks(query: CallbackQuery):
        user_id = query.from_user.id
        message_id = query.message.message_id

        if message_id not in meetings:
            await query.answer("Зустріч неактивна", show_alert=True)
            return

        # ---------------------------
        # Видалення (адмін)
        # ---------------------------
        if query.data == "delete":
            if user_id != ADMIN_ID:
                await query.answer("Тільки адміністратор", show_alert=True)
                return

            await query.message.delete()
            del meetings[message_id]
            await query.answer("Видалено ✅")
            return

        # ---------------------------
        # Редагування (адмін)
        # ---------------------------
        if query.data == "edit":
            if user_id != ADMIN_ID:
                await query.answer("Тільки адміністратор", show_alert=True)
                return

            edit_state[user_id] = message_id
            await query.message.answer("Введіть новий текст:")
            await query.answer()
            return

        # ---------------------------
        # Запис самостійно
        # ---------------------------
        if query.data == "join":
            meetings[message_id]["participants"][user_id] = []
            await query.answer("Ти записаний ✅")

        # ---------------------------
        # Додати пару
        # ---------------------------
        if query.data == "add_pair":
            existing = [
                uid for uid in meetings[message_id]["participants"]
                if uid != user_id
            ]

            if existing:
                builder = InlineKeyboardBuilder()
                for uid in existing:
                    builder.add(
                        InlineKeyboardButton(
                            text=f"Пара з {uid}",
                            callback_data=f"pair:{uid}"
                        )
                    )

                builder.add(
                    InlineKeyboardButton(
                        text="Ввести вручну",
                        callback_data="pair:manual"
                    )
                )

                await query.message.answer(
                    "Оберіть пару:",
                    reply_markup=builder.as_markup()
                )
            else:
                pair_state[user_id] = message_id
                await query.message.answer("Введіть ім'я пари:")

            await query.answer()
            return

        # ---------------------------
        # Вибір пари
        # ---------------------------
        if query.data.startswith("pair:"):
            value = query.data.split(":")[1]

            if value == "manual":
                pair_state[user_id] = message_id
                await query.message.answer("Введіть ім'я пари:")
            else:
                meetings[message_id]["participants"].setdefault(user_id, [])
                meetings[message_id]["participants"][user_id].append(
                    f"пара з {value}"
                )
                await query.answer("Пара додана ✅")

        # ---------------------------
        # Оновлення списку
        # ---------------------------
        await update_message(bot, query.message)

    # ---------------------------
    # Текстовий ввод
    # ---------------------------
    @dp.message()
    async def text_input(message: Message):
        user_id = message.from_user.id

        # Редагування адміністратором
        if user_id in edit_state:
            msg_id = edit_state.pop(user_id)
            await bot.edit_message_text(
                message.text,
                chat_id=message.chat.id,
                message_id=msg_id,
                reply_markup=default_keyboard()
            )
            await message.answer("Оновлено ✅")
            return

        # Ручне введення пари
        if user_id in pair_state:
            msg_id = pair_state.pop(user_id)
            meetings[msg_id]["participants"].setdefault(user_id, [])
            meetings[msg_id]["participants"][user_id].append(message.text)

            await message.answer("Пара додана ✅")
            await update_message(bot, message.chat.get_message(msg_id))

    # ---------------------------
    # Паралельний запуск
    # ---------------------------
    await asyncio.gather(
        dp.start_polling(bot),
        start_health_server()
    )

# =====================================================
# ДОПОМІЖНІ ФУНКЦІЇ
# =====================================================


def default_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Записатись", callback_data="join")],
        [InlineKeyboardButton(text="Додати пару", callback_data="add_pair")],
        [InlineKeyboardButton(text="Редагувати", callback_data="edit")],
        [InlineKeyboardButton(text="Видалити", callback_data="delete")]
    ])


async def update_message(bot, message):
    message_id = message.message_id
    if message_id not in meetings:
        return

    participants = meetings[message_id]["participants"]

    lines = []
    for uid, pairs in participants.items():
        lines.append(f"- <a href='tg://user?id={uid}'>Учасник</a>")
        for p in pairs:
            lines.append(f"   • {p}")

    text = "🕒 <b>Нова зустріч</b>\nЧас: 15:00\n\n<b>Учасники:</b>\n"
    text += "\n".join(lines) if lines else "Поки що немає"

    await bot.edit_message_text(
        text,
        chat_id=message.chat.id,
        message_id=message_id,
        reply_markup=default_keyboard()
    )

# =====================================================

if __name__ == "__main__":
    asyncio.run(main())
