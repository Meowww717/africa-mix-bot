from telethon import TelegramClient
import asyncio
import os
import sys

# Фікс для Windows консолі
sys.stdout.reconfigure(encoding='utf-8')

api_id = int(os.getenv("TELEGRAM_API_ID"))
api_hash = os.getenv("TELEGRAM_API_HASH")


group = "https://t.me/+sHiPUqG2ApY5MTgy"

client = TelegramClient("session", api_id, api_hash)


async def get_users():
    await client.start()
    participants = await client.get_participants(group)
    for u in participants:
        print(u.id, u.first_name, u.username)

asyncio.run(get_users())
