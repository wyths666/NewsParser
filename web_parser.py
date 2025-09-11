# web_parser.py
import sqlite3
import asyncio
import requests
import logging
import time
import random
from parsers.cnet import fetch_text_cnet
from parsers.compweekly import fetch_text_computerweekly
from parsers.engadget import fetch_text_engadget
from parsers.wired import fetch_text_wired
# --- НАСТРОЙКИ ---
DB_NAME = 'news.db'
REQUEST_TIMEOUT = 20
# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# --- СЛОВАРЬ СООТВЕТСТВИЯ ДОМЕНОВ И ФУНКЦИЙ ПАРСИНГА ---
# Ключ - домен или часть URL, значение - функция парсинга
PARSERS_FOR_SITES = {
    'wired.com': fetch_text_wired,
    'cnet.com': fetch_text_cnet,
    'computerweekly.com': fetch_text_computerweekly,
    'engadget.com': fetch_text_engadget
}

# --- ФУНКЦИИ РАБОТЫ С БД ---
def update_news_status_to_failed(db_name: str, title: str):
    """
    Обновляет статус новости на 'fetch_failed'.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    try:
        cursor.execute(
            "UPDATE news SET published = 'fetch_failed' WHERE title = ?",
            (title,)
        )
        conn.commit()
        if cursor.rowcount > 0:
            logger.info(f"Статус новости '{title[:50]}...' обновлён на 'fetch_failed'.")
        else:
            logger.warning(f"Новость '{title[:50]}...' не найдена при попытке обновить статус на 'fetch_failed'.")
    except sqlite3.Error as e:
        logger.error(f"Ошибка БД при обновлении статуса новости '{title[:50]}...' на 'fetch_failed': {e}")
        conn.rollback()
    finally:
        conn.close()

def init_db_for_full_text(db_name: str):
    """Добавляет поле full_text в таблицу news, если оно отсутствует."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    try:
        cursor.execute("PRAGMA table_info(news)")
        columns = [info[1] for info in cursor.fetchall()]
        if 'full_text' not in columns:
            logger.info("Добавление столбца 'full_text' в таблицу 'news'...")
            cursor.execute("ALTER TABLE news ADD COLUMN full_text TEXT")
            conn.commit()
            logger.info("Столбец 'full_text' успешно добавлен.")
        else:
            logger.info("Столбец 'full_text' уже существует.")
    except sqlite3.Error as e:
        logger.error(f"Ошибка при инициализации БД для полного текста: {e}")
    finally:
        conn.close()

def get_unfetched_news(db_name: str) -> list:
    """Получает список новостей, полный текст которых еще не извлечен (published = 'no')."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    try:
        # Выбираем только необходимые поля
        cursor.execute("SELECT title, link, source FROM news WHERE published = 'no'")
        rows = cursor.fetchall()
        return [{'title': row[0], 'link': row[1], 'source': row[2]} for row in rows]
    except sqlite3.Error as e:
        logger.error(f"Ошибка при получении новостей из БД: {e}")
        return []
    finally:
        conn.close()

def update_news_full_text(db_name: str, title: str, full_text: str):
    """
    Обновляет полный текст новости и устанавливает статус 'fetched'.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    try:
        cursor.execute(
            "UPDATE news SET full_text = ?, published = 'fetched' WHERE title = ?",
            (full_text, title)
        )
        conn.commit()
        if cursor.rowcount > 0:
            logger.info(f"✔️ Новость '{title}' обновлена в БД (статус: fetched).")
        else:
            logger.warning(f"Новость '{title}' не найдена в БД при попытке обновления full_text.")
    except sqlite3.Error as e:
        logger.error(f"Ошибка БД при обновлении full_text новости '{title}': {e}")
        conn.rollback()
    finally:
        conn.close()

# --- ОСНОВНАЯ ЛОГИКА ---

def fetch_full_texts():
    """Основная функция извлечения полного текста для новостей."""
    logger.info("=== Начало извлечения полного текста статей ===")

    # 1. Инициализируем БД (добавляем full_text, если нужно)
    init_db_for_full_text(DB_NAME)

    # 2. Получаем список новостей для обработки
    unfetched_news = get_unfetched_news(DB_NAME)

    if not unfetched_news:
        logger.info("Нет новостей для извлечения полного текста.")
        return

    logger.info(f"Найдено {len(unfetched_news)} новостей для обработки.")
    fetched_count = 0

    # 3. Обрабатываем каждую новость
    for news_item in unfetched_news:
        title = news_item['title']
        link = news_item['link']
        source = news_item['source'] # Можно использовать для логгирования

        logger.info(f"--- Обработка: {title[:50]}... ---")

        # 4. Определяем, какой парсер использовать на основе URL
        parser_func = None
        for domain, func in PARSERS_FOR_SITES.items():
            if domain in link:
                parser_func = func
                break

        if not parser_func:
            logger.warning(f"Не найден парсер для домена в URL: {link}. Пропуск.")
            # Можно обновить статус на 'no_parser' или подобное
            continue

        # 5. Извлекаем текст с помощью выбранного парсера
        full_text = None
        try:
            full_text = parser_func(link)
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка сети при получении статьи {link}: {e}")
            # Продолжаем цикл, переходя к следующей новости
            continue
        except Exception as e:
            # Ловим любые другие исключения, которые могут возникнуть в парсере
            logger.error(f"Неожиданная ошибка при парсинге статьи {link}: {e}", exc_info=True)
            # Продолжаем цикл, переходя к следующей новости
            continue

        if not full_text:
            logger.warning(f"Парсер вернул пустой текст для статьи '{title}'. Обновление статуса.")
            # Обновляем статус новости на 'fetch_failed'
            update_news_status_to_failed(DB_NAME, title)
            continue  # Переход к следующей новости
            # --- КОНЕЦ ИЗМЕНЕНИЯ ---

            # 6. Сохраняем текст в БД и обновляем статус на 'fetched'
        update_news_full_text(DB_NAME, title, full_text)
        fetched_count += 1
        # 7. Небольшая задержка между запросами
        time.sleep(random.uniform(1, 2)) # Используем random для более естественной задержки

    logger.info(f"=== ✔️ Извлечение текста завершено. Обработано: {fetched_count} ===")




