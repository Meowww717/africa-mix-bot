import asyncio
import os
from aiogram import Bot, Dispatcher
from aiogram.types import Message
from aiogram.filters import Command

TOKEN = os.getenv("TOKEN")
ADMIN_ID = 561261084

if not TOKEN:
    raise ValueError("TOKEN not found!")


async def main():
    bot = Bot(token=TOKEN)
    dp = Dispatcher()

    @dp.message(Command("ping"))
    async def ping(message: Message):
        if message.from_user.id != ADMIN_ID:
            return  # тихий ігнор

        await message.answer("Я працюю в групі ✅")

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
