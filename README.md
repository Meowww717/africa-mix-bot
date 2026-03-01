# 📅 Telegram Meeting Bot

Бот для організації зустрічей у Telegram-групі.

## 🚀 Функціонал

✅ Адміністратор може:

- Створювати зустріч (/meeting)
- Редагувати повідомлення про зустріч
- Видаляти зустріч

✅ Користувачі можуть:

- Записатися самостійно
- Додати пару:
  - Обрати зі списку учасників
  - Ввести ім'я вручну

✅ Повідомлення автоматично оновлюється зі списком учасників.

---

# 🛠 Технології

- Python 3.10+
- aiogram v3
- Fly.io (деплой)
- GitHub (опціонально для автодеплою)

---

# 📦 Встановлення локально

## 1️⃣ Клонувати репозиторій

```bash
git clone https://github.com/your-username/your-repo.git
cd your-repo
```

## 2️⃣ Створити віртуальне середовище

```bash
python -m venv venv
source venv/bin/activate  # Mac/Linux
venv\Scripts\activate     # Windows
```

## 3️⃣ Встановити залежності

```bash
pip install -r requirements.txt
```

Якщо `requirements.txt` немає:

```bash
pip install aiogram
```

---

# 🔑 Налаштування змінних середовища

Потрібно створити токен бота у @BotFather.

Далі встановити змінну середовища:

### Mac/Linux:

```bash
export TOKEN=your_bot_token_here
```

### Windows:

```bash
set TOKEN=your_bot_token_here
```

---

# ▶️ Запуск локально

```bash
python main.py
```

Якщо все працює — бот відповість на `/ping`.

---

# ☁️ Деплой на Fly.io

## 1️⃣ Логін

```bash
fly auth login
```

## 2️⃣ Ініціалізація (якщо перший раз)

```bash
fly launch
```

## 3️⃣ Деплой

```bash
fly deploy
```

Після кожної зміни коду потрібно виконувати:

```bash
fly deploy
```

---

# 🔄 Автоматичний деплой через GitHub (опціонально)

Можна додати GitHub Actions для автодеплою після `git push`.

1. Створити `FLY_API_TOKEN` у Fly:

```bash
fly tokens create deploy
```

2. Додати його у GitHub → Settings → Secrets → `FLY_API_TOKEN`

3. Створити файл:
   `.github/workflows/deploy.yml`

(дивись приклад у документації Fly)

Тепер після пушу в `main` деплой буде автоматичний 🚀

---

# 📂 Структура проекту

```
.
├── main.py
├── requirements.txt
├── fly.toml
└── README.md
```

---

# ⚠️ Важливо

- Бот повинен бути доданий у групу
- Боту потрібні права на:
  - Видалення повідомлень
  - Редагування повідомлень
- ADMIN_ID має бути твій реальний Telegram ID

---

# 💡 Можливі покращення

- Збереження даних у базу (SQLite / PostgreSQL)
- Обмеження по кількості учасників
- Автоматичне закриття реєстрації
- Нотифікації перед зустріччю

---

# 👩‍💻 Автор

Valentyna ✨
