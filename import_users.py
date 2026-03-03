import asyncio
import sqlite3
from telethon import TelegramClient

api_id = 33835492
api_hash = "8085a2ec391ff98c8185cf0f741945eb"
group = "https://t.me/+sHiPUqG2ApY5MTgy"

# Підключаємось до тієї ж бази що і бот
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
conn.commit()

client = TelegramClient("session", api_id, api_hash)


async def import_users():
    await client.start()
    participants = await client.get_participants(group)

    added = 0
    skipped = 0

    for u in participants:
        if u.bot:
            continue  # пропускаємо ботів

        # Перевіряємо чи вже є в базі
        cursor.execute("SELECT user_id FROM users WHERE user_id=?", (u.id,))
        if cursor.fetchone():
            skipped += 1
            continue

        first_name = u.first_name or ""
        last_name = u.last_name or ""
        username = u.username or ""

        # gender треба вказати вручну або залишаємо пустим
        cursor.execute("""
            INSERT OR IGNORE INTO users(user_id, first_name, last_name, username, gender)
            VALUES (?, ?, ?, ?, ?)
        """, (u.id, first_name, last_name, username, "male"))
        added += 1

    conn.commit()
    print(f"✅ Додано: {added} | Пропущено (вже є): {skipped}")

    # Показуємо всіх в базі
    cursor.execute("SELECT user_id, first_name, username FROM users")
    rows = cursor.fetchall()
    print("\n📋 Юзери в базі:")
    for row in rows:
        print(f"  ID: {row[0]} | {row[1]} | @{row[2]}")

asyncio.run(import_users())
