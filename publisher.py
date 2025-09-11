# publisher.py
import asyncio
import sqlite3
import random
import logging
from aiogram import Bot, types
from aiogram.exceptions import TelegramAPIError
from dotenv import load_dotenv
import os
import datetime

def get_sleep_duration():
    """Рассчитывает сколько секунд спать до 7 утра"""
    now = datetime.datetime.now()

    # Если сейчас ночное время
    if datetime.time(2, 0) <= now.time() < datetime.time(7, 0):
        # Создаем datetime на 7:00 сегодня
        wakeup_time = now.replace(hour=7, minute=0, second=0, microsecond=0)
        # Если уже прошло 7:00, берем 7:00 следующего дня
        if now >= wakeup_time:
            wakeup_time += datetime.timedelta(days=1)

        sleep_seconds = (wakeup_time - now).total_seconds()
        return sleep_seconds

    return 0  # Не ночное время
# --- НАСТРОЙКИ ---
DB_NAME = 'news.db'
load_dotenv()
BOT_TOKEN = os.getenv('BOT_TOKEN')
CHANNEL_ID = os.getenv('CHANNEL_ID')

if not BOT_TOKEN or not CHANNEL_ID:
    raise ValueError("TELEGRAM_BOT_TOKEN и/или TELEGRAM_CHANNEL_ID не установлены.")

# Интервал публикации в минутах
PUBLISH_INTERVAL_MIN = 10
PUBLISH_INTERVAL_MAX = 20

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- ФУНКЦИИ РАБОТЫ С БД ---
def get_next_processed_news(db_name: str) -> dict | None:
    """
    Получает следующую новость со статусом 'processed' из БД.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    try:
        # Выбираем новость со статусом 'processed'
        cursor.execute(
            "SELECT title, title_ru, processed_full_text, link, source FROM news WHERE published = 'processed' ORDER BY RANDOM() LIMIT 1"
        )
        row = cursor.fetchone()
        if row:
            return {
                'title': row[0],
                'title_ru': row[1],
                'processed_full_text': row[2],
                'link': row[3],
                'source': row[4]

            }
        else:
            return None
    except sqlite3.Error as e:
        logger.error(f"Ошибка БД при получении новости: {e}")
        return None
    finally:
        conn.close()

def mark_news_as_published(db_name: str, title: str) -> bool:
    """
    Обновляет статус новости на 'published_to_tg'.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    try:
        cursor.execute(
            "UPDATE news SET published = 'published_to_tg' WHERE title = ?",
            (title,)
        )
        conn.commit()
        success = cursor.rowcount > 0
        if success:
            logger.info(f"Новость '{title[:50]}...' помечена как опубликованная в Telegram.")
        else:
            logger.warning(f"Не удалось обновить статус новости '{title[:50]}...'. Запись не найдена.")
        return success
    except sqlite3.Error as e:
        logger.error(f"Ошибка БД при обновлении статуса новости '{title[:50]}...': {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

# --- ФУНКЦИЯ ПУБЛИКАЦИИ В TELEGRAM ---
async def publish_news_to_telegram(bot: Bot, news_item: dict):
    """
    Публикует новость в Telegram-канал.
    """
    try:
        title_ru = news_item['title_ru']
        source = news_item['source']
        processed_text = news_item['processed_full_text']
        link = news_item['link']
        emozi = ['📰', '📄', '♨️', '‼️', '⭐', '⚡', '💥', '🧨', '🎉', '🌟', '✨', '📨', '❗']

        if not title_ru or not processed_text:
            logger.warning(f"Новость '{news_item['title'][:50]}...' не содержит переведенного заголовка или текста. Пропуск публикации.")
            # Все равно помечаем как опубликованную, чтобы не зацикливалось
            mark_news_as_published(DB_NAME, news_item['title'])
            return
        if 'vpn' in processed_text.lower(): # запрещена реклама vpn сервисов
            logger.info(f"Пропущена публикация: обнаружено слово 'vpn' в тексте новости '{news_item['title'][:50]}...'")
            mark_news_as_published(DB_NAME, news_item['title'])
            return
        if 'В интернете есть много сайтов с информацией на эту тему.' in processed_text:
            logger.info(f"Пропущена публикация: ошибка обработки gpt новости '{news_item['title'][:50]}...'")
            mark_news_as_published(DB_NAME, news_item['title'])
            return

        # Формируем текст сообщения
        # Используем HTML для форматирования
        message_text = f"{random.choice(emozi)}<b>{source}: {title_ru}</b>\n\n{processed_text}\n\n🔗 <a href='{link}'>Читать оригинал новости в источнике 👇</a>"

        # Ограничение длины сообщения Telegram (4096 символов для текста)
        # Если превышает, можно обрезать или отправить как документ/статью (не реализовано здесь)
        MAX_TELEGRAM_MESSAGE_LENGTH = 4000 # Оставляем запас
        if len(message_text) > MAX_TELEGRAM_MESSAGE_LENGTH:
            logger.warning(f"Текст новости '{title_ru[:30]}...' слишком длинный ({len(message_text)} символов). Обрезаем.")
            # Обрезаем текст, оставляя место для заголовка, ссылки и ...
            # Простая логика обрезки
            available_text_length = MAX_TELEGRAM_MESSAGE_LENGTH - len(f"<b>{title_ru}</b>\n\n...\n\n🔗 <a href='{link}'>Читать оригинал</a>")
            if available_text_length > 100:
                processed_text_trimmed = processed_text[:available_text_length - 3] + "..."
                message_text = f"<b>{title_ru}</b>\n\n{processed_text_trimmed}\n\n🔗 <a href='{link}'>Читать оригинал</a>"
            else:
                 # Если совсем мало места, отправляем только заголовок и ссылку
                 message_text = f"<b>{title_ru}</b>\n\n🔗 <a href='{link}'>Читать оригинал</a>"
                 logger.warning("Очень короткий доступный лимит, отправляем только заголовок и ссылку.")


        # Отправляем сообщение
        await bot.send_message(
            chat_id=CHANNEL_ID,
            text=message_text,
            parse_mode='HTML',
            disable_web_page_preview=False # Отключаем предпросмотр ссылки если нужно
        )
        logger.info(f"Новость '{title_ru[:50]}...' успешно опубликована в Telegram.")

        # Обновляем статус в БД
        mark_news_as_published(DB_NAME, news_item['title'])

    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API при публикации новости '{news_item['title_ru'][:50]}...': {e}")
        # Можно решить, помечать ли новость как опубликованную при ошибке Telegram
        # Например, если ошибка "Bad Request: message is too long", то да, иначе - нет.
        # Для простоты, не будем менять статус, пусть повторит попытку позже.
    except Exception as e:
        logger.error(f"Неожиданная ошибка при публикации новости '{news_item['title_ru'][:50]}...': {e}", exc_info=True)
        # Не меняем статус, чтобы повторить попытку

# --- ОСНОВНОЙ ЦИКЛ ПУБЛИКАЦИИ ---
async def run_publisher():
    """
    Основная асинхронная функция публикации новостей.
    """

    logger.info("=== Запуск Telegram Publisher ===")
    bot = Bot(token=BOT_TOKEN)

    try:
        while True:
            sleep_duration = get_sleep_duration()
            if sleep_duration > 0:
                logger.info(f'🌙 Ночной перерыв. Ожидание до утра: {sleep_duration / 3600:.1f} часов')
                await asyncio.sleep(sleep_duration)
                continue
            # 1. Получаем следующую обработанную новость
            news_item = get_next_processed_news(DB_NAME)

            if news_item:
                # 2. Если новость найдена, публикуем её
                await publish_news_to_telegram(bot, news_item)
            else:
                logger.info("Нет новых обработанных новостей для публикации.")

            # 3. Рассчитываем случайную задержку
            delay_minutes = random.randint(PUBLISH_INTERVAL_MIN, PUBLISH_INTERVAL_MAX)
            delay_seconds = delay_minutes * 60
            logger.info(f"Ожидание {delay_minutes} минут до следующей публикации...")

            # 4. Ждем
            await asyncio.sleep(delay_seconds)

    except asyncio.CancelledError:
        logger.info("Publisher task was cancelled.")
    except Exception as e:
        logger.critical(f"Критическая ошибка в основном цикле publisher: {e}", exc_info=True)
    finally:
        await bot.session.close()
        logger.info("=== Telegram Publisher остановлен ===")

# --- ЗАПУСК ---
if __name__ == "__main__":
    try:
        asyncio.run(run_publisher())
    except KeyboardInterrupt:
        logger.info("Получен сигнал завершения (Ctrl+C). Закрываем publisher...")