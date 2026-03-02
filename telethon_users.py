from telethon import TelegramClient
import asyncio

api_id = 33835492
api_hash = "8085a2ec391ff98c8185cf0f741945eb"
group = "https://t.me/+sHiPUqG2ApY5MTgy"


client = TelegramClient("session", api_id, api_hash)


async def get_users():
    await client.start()
    participants = await client.get_participants(group)
    for u in participants:
        print(u.id, u.first_name, u.username)

asyncio.run(get_users())
