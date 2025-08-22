import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any, Callable, Optional
import time
import random
import sqlite3
import asyncio
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- НАСТРОЙКИ ---
RSS_FEEDS = {
    'WIRED Science': 'https://www.wired.com/feed/category/science/latest/rss',
    'WIRED Business': 'https://www.wired.com/feed/category/business/latest/rss',
    'Computer Weekly': 'https://www.computerweekly.com/rss/All-Computer-Weekly-content.xml',
    'CNET': 'https://www.cnet.com/rss/news/',
    'Engadget': 'https://www.engadget.com/rss.xml'
}
# Имя файла базы данных
DB_NAME = 'news.db'


# --- РАБОТА С БАЗОЙ ДАННЫХ ---
def init_db(db_name: str):
    """Создает таблицу новостей, если она не существует."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Создаем таблицу с полями title (уникальный), link, description, thumbnail_url, source, published
    # title будет PRIMARY KEY для простоты проверки дубликатов
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS news (
            title TEXT PRIMARY KEY,
            link TEXT NOT NULL,
            thumbnail_url TEXT,
            source TEXT NOT NULL,
            published TEXT NOT NULL DEFAULT 'no'
        )
    ''')
    conn.commit()
    conn.close()
    logger.info(f"База данных '{db_name}' инициализирована.")


def save_news_to_db(db_name: str, news_item: Dict[str, Any]) -> bool:
    """
    Сохраняет одну новость в базу данных, если её ещё нет.
    Возвращает True, если новость была добавлена, False - если дубликат.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    try:
        # Используем INSERT OR IGNORE для предотвращения ошибки при дубликате
        # Так как title - PRIMARY KEY, дубликат вызовет IntegrityError,
        # которую мы подавляем с помощью IGNORE
        cursor.execute('''
            INSERT OR IGNORE INTO news 
            (title, link, thumbnail_url, source) 
            VALUES (?, ?, ?, ?)
        ''', (
            news_item['title'],
            news_item['link'],
            news_item['thumbnail_url'],
            news_item['source']
        ))

        conn.commit()
        # Если rowcount == 1, значит строка была вставлена
        # Если rowcount == 0, значит строка с таким title уже была (дубликат)
        rows_affected = cursor.rowcount
        return rows_affected > 0

    except sqlite3.Error as e:
        logger.error(f"Ошибка базы данных при сохранении новости '{news_item['title']}': {e}")
        conn.rollback()
        return False
    finally:
        conn.close()


# --- КОНФИГУРАЦИЯ ПАРСИНГА ---
def parse_item_default(item, source_name: str) -> Optional[Dict[str, Any]]:
    """Универсальный парсер элемента RSS. Сохраняет только URL миниатюры."""
    try:
        title_tag = item.find('title')
        link_tag = item.find('link')

        entry = {
            'title': title_tag.get_text(strip=True) if title_tag else None,
            'link': link_tag.get_text(strip=True) if link_tag else None,
            'thumbnail_url': None,
            'source': source_name
        }

        # Парсинг <media:thumbnail> - только URL
        thumbnail_tag = item.find('media:thumbnail')
        if thumbnail_tag and thumbnail_tag.get('url'):
            entry['thumbnail_url'] = thumbnail_tag.get('url').strip()

        return entry
    except Exception as e:
        logger.error(f"Ошибка при парсинге элемента из {source_name} (default parser): {e}")
        return None


def parse_item_cnet(item, source_name: str) -> Optional[Dict[str, Any]]:
    """Парсер для CNET. Использует media:content для изображений, сохраняет только URL."""
    try:
        entry = parse_item_default(item, source_name)
        if not entry:
            return None

        # Если стандартный thumbnail_url не найден, попробуем media:content
        if not entry.get('thumbnail_url'):
            # Ищем media:content с medium="image"
            content_tag = item.find('media:content', attrs={"medium": "image"})
            if content_tag and content_tag.get('url'):
                entry['thumbnail_url'] = content_tag.get('url', '').strip()

        return entry
    except Exception as e:
        logger.error(f"Ошибка при парсинге элемента из {source_name} (CNET parser): {e}")
        return None

def parse_item_copmweek(item, source_name: str) -> Optional[Dict[str, Any]]:
    try:
        entry = parse_item_default(item, source_name)
        if not entry:
            return None

        if not entry.get('thumbnail_url'):
            content_tag = item.find('image')
            entry['thumbnail_url'] = content_tag.get_text(strip=True)

        return entry
    except Exception as e:
        logger.error(f"Ошибка при парсинге элемента из {source_name} (CNET parser): {e}")
        return None


# Словарь сопоставления источника и его функции парсинга
PARSERS = {
    'CNET': parse_item_cnet, 'Computer Weekly': parse_item_copmweek,
'Engadget': parse_item_cnet
    # По умолчанию используется parse_item_default
}


# --- ЛОГИКА ПАРСИНГА ---
def parse_single_rss_feed(source_name: str, rss_url: str) -> List[Dict[str, Any]]:
    """
    Парсит один RSS-фид с учетом специфики источника.
    """
    try:
        logger.info(f"Получение данных из: {source_name} ({rss_url})")
        # Добавим User-Agent, чтобы избежать блокировок
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(rss_url.strip(), timeout=15, headers=headers)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'xml')

        news = []
        items = soup.find_all('item')
        logger.info(f"Найдено {len(items)} элементов в {source_name}")

        parser_func: Callable = PARSERS.get(source_name, parse_item_default)

        for item in items:
            entry = parser_func(item, source_name)
            if entry:
                news.append(entry)

        logger.info(f"✔️ Успешно обработано {len(news)} новостей из {source_name}")
        return news

    except requests.exceptions.RequestException as e:
        print(f"Ошибка сети при запросе {source_name} ({rss_url}): {e}")
        return []
    except Exception as e:
        import traceback
        logger.error(f"Ошибка парсинга {source_name} ({rss_url}): {e}")
        # traceback.print_exc()
        return []


def parse_and_save_multiple_rss_feeds(rss_feeds_dict: Dict[str, str], db_name: str) -> int:
    """
    Парсит несколько RSS-фидов и сохраняет уникальные новости в базу данных.
    Возвращает количество добавленных новостей.
    """
    added_count = 0
    feed_items = list(rss_feeds_dict.items())
    random.shuffle(feed_items)

    for name, url in feed_items:
        news_from_source = parse_single_rss_feed(name, url)
        for item in news_from_source:
            # Пытаемся сохранить новость в БД
            is_new = save_news_to_db(db_name, item)
            if is_new:
                added_count += 1
        time.sleep(random.uniform(0.5, 2.0))

    logger.info(f"\n✔️ Парсинг завершен. Добавлено новых новостей в БД: {added_count}")
    return added_count


# --- ОСНОВНАЯ ЧАСТЬ ---
def start_parsing():
    # 1. Инициализируем базу данных
    init_db(DB_NAME)

    # 2. Парсим ленты и сохраняем в БД
    logger.info(f"Начинаем парсинг RSS-лент и сохранение в БД...")
    total_added = parse_and_save_multiple_rss_feeds(RSS_FEEDS, DB_NAME)


