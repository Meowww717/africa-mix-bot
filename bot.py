import asyncio
import os
from aiogram import Bot, Dispatcher
from aiogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
from aiogram.filters import Command, Text
from aiogram.utils.keyboard import InlineKeyboardBuilder

# ===============================
# Налаштування
# ===============================
TOKEN = os.getenv("TOKEN")  # Ваш токен бота з @BotFather
ADMIN_ID = 561261084        # Ваш Telegram ID як адміна

if not TOKEN:
    raise ValueError("TOKEN not found!")

# ===============================
# Збереження стану
# ===============================
# Зустрічі: {message_id: {"text": текст повідомлення, "participants": {user_id: [pair_list]}}}
meetings = {}

# Стан редагування тексту адміністратором: {user_id: message_id}
edit_state = {}

# Стан для введення пари вручну: {user_id: message_id}
pair_state = {}

# ===============================
# Основна функція запуску бота
# ===============================


async def main():
    # Використовуємо HTML для форматування
    bot = Bot(token=TOKEN, parse_mode="HTML")
    dp = Dispatcher()

    # -------------------------------
    # Команда /ping для перевірки роботи
    # -------------------------------
    @dp.message(Command("ping"))
    async def ping_handler(message: Message):
        if message.from_user.id != ADMIN_ID:
            return
        await message.answer("Я працюю в групі ✅")

    # -------------------------------
    # Команда /meeting для створення зустрічі адміністратором
    # -------------------------------
    @dp.message(Command("meeting"))
    async def meeting_handler(message: Message):
        if message.from_user.id != ADMIN_ID:
            return

        # Створюємо клавіатуру з кнопками:
        # - Записатись самостійно
        # - Додати пару
        # - Редагувати повідомлення (для адміна)
        # - Видалити зустріч (для адміна)
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="Записатись", callback_data="join_self")],
            [InlineKeyboardButton(text="Додати пару",
                                  callback_data="join_pair")],
            [InlineKeyboardButton(text="Редагувати (адмін)",
                                  callback_data="edit_meeting")],
            [InlineKeyboardButton(
                text="Видалити зустріч (адмін)", callback_data="delete_meeting")]
        ])

        # Відправляємо повідомлення про нову зустріч
        msg = await message.answer(
            "🕒 <b>Нова зустріч</b>\nЧас: 15:00\nНатисни кнопку нижче, щоб записатись",
            reply_markup=keyboard
        )

        # Ініціалізуємо словник учасників для цієї зустрічі
        meetings[msg.message_id] = {"text": msg.text, "participants": {}}

    # -------------------------------
    # Обробка натискання кнопок
    # -------------------------------
    @dp.callback_query(Text(startswith="join") | Text(startswith="edit_meeting") | Text(startswith="select_pair") | Text(startswith="delete_meeting"))
    async def callback_handler(query: CallbackQuery):
        user_id = query.from_user.id
        message_id = query.message.message_id

        # -------------------------------
        # 1️⃣ Редагування повідомлення адміністратором
        # -------------------------------
        if query.data == "edit_meeting":
            if user_id != ADMIN_ID:
                await query.answer("Тільки адміністратор може редагувати", show_alert=True)
                return
            await query.message.answer("Введи новий текст для повідомлення про зустріч:")
            edit_state[user_id] = message_id
            await query.answer()
            return

        # -------------------------------
        # 2️⃣ Видалення зустрічі адміністратором
        # -------------------------------
        if query.data == "delete_meeting":
            if user_id != ADMIN_ID:
                await query.answer("Тільки адміністратор може видаляти зустріч", show_alert=True)
                return
            # Видаляємо повідомлення з чату
            await bot.delete_message(chat_id=query.message.chat.id, message_id=message_id)
            # Видаляємо зі словника зустрічей
            if message_id in meetings:
                del meetings[message_id]
            await query.answer("Зустріч видалена ✅")
            return

        # Перевіряємо чи існує зустріч
        if message_id not in meetings:
            await query.answer("Ця зустріч більше не активна", show_alert=True)
            return

        # -------------------------------
        # 3️⃣ Запис користувача самостійно
        # -------------------------------
        if query.data == "join_self":
            meetings[message_id]["participants"][user_id] = []
            await query.answer("Ти записаний ✅")

        # -------------------------------
        # 4️⃣ Додати пару
        # -------------------------------
        elif query.data == "join_pair":
            # Перевіряємо існуючих учасників (крім себе)
            existing_users = [uid for uid in meetings[message_id]
                              ["participants"] if uid != user_id]
            if existing_users:
                # Будуємо клавіатуру для вибору пари
                builder = InlineKeyboardBuilder()
                for uid in existing_users:
                    builder.add(
                        InlineKeyboardButton(
                            text=f"Пара з {uid}", callback_data=f"select_pair:{uid}"
                        )
                    )
                # Кнопка для введення імені вручну
                builder.add(
                    InlineKeyboardButton(
                        text="Ввести ім'я вручну", callback_data="select_pair:manual")
                )
                await query.message.answer("Оберіть пару або введіть ім'я вручну:", reply_markup=builder.as_markup())
            else:
                # Якщо немає інших учасників, одразу запитуємо ім'я
                await query.message.answer("Введіть ім'я вашої пари:")
                pair_state[user_id] = message_id
            await query.answer()

        # -------------------------------
        # 5️⃣ Обробка вибору пари зі списку
        # -------------------------------
        elif query.data.startswith("select_pair"):
            _, selected = query.data.split(":")
            if selected == "manual":
                await query.message.answer("Введіть ім'я вашої пари:")
                pair_state[user_id] = message_id
            else:
                # Додаємо пару з існуючого учасника
                if user_id in meetings[message_id]["participants"]:
                    meetings[message_id]["participants"][user_id].append(
                        f"пара з {selected}")
                else:
                    meetings[message_id]["participants"][user_id] = [
                        f"пара з {selected}"]
                await query.answer("Пара додана ✅")

        # -------------------------------
        # Оновлюємо повідомлення з учасниками
        # -------------------------------
        participants_list = []
        for uid, pair in meetings[message_id]["participants"].items():
            participants_list.append(
                f"- <a href='tg://user?id={uid}'>User</a>")
            for p in pair:
                participants_list.append(f"  - {p}")

        new_text = f"🕒 <b>Нова зустріч</b>\nЧас: 15:00\n\n<b>Учасники:</b>\n" + \
            "\n".join(participants_list)
        await query.message.edit_text(new_text, reply_markup=query.message.reply_markup)

    # -------------------------------
    # 6️⃣ Обробка введення тексту:
    # - редагування повідомлення адміністратором
    # - введення імені пари вручну
    # -------------------------------
    @dp.message()
    async def text_handler(message: Message):
        user_id = message.from_user.id

        # Редагування адміністратором
        if user_id in edit_state:
            msg_id = edit_state.pop(user_id)
            if msg_id not in meetings:
                await message.answer("Ця зустріч вже не активна")
                return
            meetings[msg_id]["text"] = message.text
            await bot.edit_message_text(
                chat_id=message.chat.id,
                message_id=msg_id,
                text=message.text,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(
                        text="Записатись", callback_data="join_self")],
                    [InlineKeyboardButton(
                        text="Додати пару", callback_data="join_pair")],
                    [InlineKeyboardButton(
                        text="Редагувати (адмін)", callback_data="edit_meeting")],
                    [InlineKeyboardButton(
                        text="Видалити зустріч (адмін)", callback_data="delete_meeting")]
                ])
            )
            await message.answer("Повідомлення оновлено ✅")
            return

        # Введення імені пари вручну
        if user_id in pair_state:
            msg_id = pair_state.pop(user_id)
            if msg_id not in meetings:
                await message.answer("Ця зустріч вже не активна")
                return
            if user_id in meetings[msg_id]["participants"]:
                meetings[msg_id]["participants"][user_id].append(message.text)
            else:
                meetings[msg_id]["participants"][user_id] = [message.text]
            await message.answer(f"Пара '{message.text}' додана ✅")

            # Оновлюємо повідомлення
            participants_list = []
            for uid, pair in meetings[msg_id]["participants"].items():
                participants_list.append(
                    f"- <a href='tg://user?id={uid}'>User</a>")
                for p in pair:
                    participants_list.append(f"  - {p}")
            new_text = f"🕒 <b>Нова зустріч</b>\nЧас: 15:00\n\n<b>Учасники:</b>\n" + \
                "\n".join(participants_list)
            await bot.edit_message_text(
                new_text,
                chat_id=message.chat.id,
                message_id=msg_id,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(
                        text="Записатись", callback_data="join_self")],
                    [InlineKeyboardButton(
                        text="Додати пару", callback_data="join_pair")],
                    [InlineKeyboardButton(
                        text="Редагувати (адмін)", callback_data="edit_meeting")],
                    [InlineKeyboardButton(
                        text="Видалити зустріч (адмін)", callback_data="delete_meeting")]
                ])
            )

    print("Бот запущено...")
    await dp.start_polling(bot)

# ===============================
# Старт бота
# ===============================
if __name__ == "__main__":
    asyncio.run(main())
