from dotenv import load_dotenv
from telethon import TelegramClient
import asyncio
import sys

# Фікс для Windows консолі
sys.stdout.reconfigure(encoding='utf-8')


load_dotenv()

api_id = int(os.getenv("TELEGRAM_API_ID"))
api_hash = os.getenv("TELEGRAM_API_HASH")

api_id = 33835492
api_hash = "8085a2ec391ff98c8185cf0f741945eb"
group = "https://t.me/+sHiPUqG2ApY5MTgy"

client = TelegramClient("session", api_id, api_hash)


async def get_users():
    await client.start()
    participants = await client.get_participants(group)
    print("Учасники групи:\n")
    for u in participants:
        if u.bot:
            continue
        print(
            f"ID: {u.id} | {u.first_name} {u.last_name or ''} | @{u.username or '-'}")

asyncio.run(get_users())
