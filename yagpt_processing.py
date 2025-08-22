import sqlite3
from typing import Optional
import logging
import time
import os
from dotenv import load_dotenv
from yandex_cloud_ml_sdk import YCloudML

# --- НАСТРОЙКИ ---
DB_NAME = 'news.db'

# Получаем настройки из переменных окружения
load_dotenv()
YC_FOLDER_ID = os.getenv('YC_FOLDER_ID')
YC_API_KEY = os.getenv('YC_API_KEY')

if not YC_FOLDER_ID:
    raise ValueError("YC_FOLDER_ID не установлен. Пожалуйста, установите переменную окружения.")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# --- ФУНКЦИИ РАБОТЫ С YANDEX CLOUD через SDK ---
def process_text_with_yacloud_sdk(text: str) -> Optional[str]:
    """
    Отправляет текст в Yandex GPT через официальный SDK и возвращает обработанный результат.
    """
    if not text:
        return ""

    try:
        # Инициализируем SDK
        # auth можно передать напрямую как API-ключ, или использовать другие методы (например, IAM-токен)
        sdk = YCloudML(folder_id=YC_FOLDER_ID, auth=YC_API_KEY)

        # Ограничиваем длину входного текста
        max_input_len = 3000  # Можно настроить
        text_to_process = text[:max_input_len]

        # Формируем сообщения для модели
        messages = [
            {
                "role": "system",
                # Уточняем задачу: перевод + краткий пересказ, готовый к публикации
                "text": "Ты профессиональный редактор и переводчик. Переведи следующий английский текст на русский язык. Затем предоставь краткий, ясный пересказ переведенного текста на русском языке. Пересказ должен быть готов к публикации, без дополнительных ремарок, вводных слов или пояснений. Сохрани ключевые факты и смысл. Сохрани все имена собственные (людей, компаний, продуктов) в оригинальном английском виде. Добавь подходящие по смылу эмодзи между абзацами",
            },
            {
                "role": "user",
                "text": text_to_process,
            },
        ]

        logger.info("Отправка запроса к Yandex GPT через SDK...")
        # Выбираем модель (yandexgpt или yandexgpt-lite) и настраиваем параметры
        # yandexgpt-lite быстрее и дешевле, подходит для большинства задач
        result = (
            sdk.models.completions("yandexgpt-lite")  # Или "yandexgpt" для более мощной модели
            .configure(
                temperature=0.5,  # Более детерминированный результат
                max_tokens=1000  # Ограничиваем длину ответа
            )
            .run(messages)
        )

        # Обрабатываем результат
        # result - это итератор по альтернативам, обычно берем первую (самую вероятную)
        for alternative in result:
            # alternative.text содержит сгенерированный текст
            logger.info("✔️ Обработка Yandex GPT через SDK завершена.")
            return alternative.text.strip()

        # Если альтернатив не было
        logger.warning("Yandex GPT SDK вернул пустой результат.")
        return None

    except Exception as e:
        logger.error(f"Ошибка при обработке текста через Yandex Cloud SDK: {e}", exc_info=True)
        return None


# --- ФУНКЦИИ РАБОТЫ С БД (копируем из предыдущего кода) ---
def init_db_for_processed_text(db_name: str):
    """Добавляет поля title_ru и processed_full_text в таблицу news, если они отсутствуют."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    try:
        cursor.execute("PRAGMA table_info(news)")
        columns = [info[1] for info in cursor.fetchall()]

        if 'title_ru' not in columns:
            logger.info("Добавление столбца 'title_ru' в таблицу 'news'...")
            cursor.execute("ALTER TABLE news ADD COLUMN title_ru TEXT")
            conn.commit()
            logger.info("Столбец 'title_ru' успешно добавлен.")
        else:
            logger.info("Столбец 'title_ru' уже существует.")

        if 'processed_full_text' not in columns:
            logger.info("Добавление столбца 'processed_full_text' в таблицу 'news'...")
            cursor.execute("ALTER TABLE news ADD COLUMN processed_full_text TEXT")
            conn.commit()
            logger.info("Столбец 'processed_full_text' успешно добавлен.")
        else:
            logger.info("Столбец 'processed_full_text' уже существует.")

    except sqlite3.Error as e:
        logger.error(f"Ошибка при инициализации БД для обработанного текста: {e}")
    finally:
        conn.close()


def get_fetched_news(db_name: str):
    """Получает список новостей, полный текст которых извлечен (published = 'fetched')."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT title, full_text FROM news WHERE published = 'fetched'")
        rows = cursor.fetchall()
        logger.info(f"Найдено {len(rows)} новостей для обработки (статус 'fetched').")
        return [{'title': row[0], 'full_text': row[1]} for row in rows]
    except sqlite3.Error as e:
        logger.error(f"Ошибка при получении новостей из БД: {e}")
        return []
    finally:
        conn.close()


def update_news_processed_text(db_name: str, original_title: str, title_ru: str, processed_full_text: str):
    """
    Обновляет переведенный заголовок и обработанный полный текст новости и устанавливает статус 'processed'.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    try:
        cursor.execute(
            "UPDATE news SET title_ru = ?, processed_full_text = ?, published = 'processed' WHERE title = ?",
            (title_ru, processed_full_text, original_title)
        )
        conn.commit()
        if cursor.rowcount > 0:
            logger.info(f"✔️ Новость '{original_title[:50]}...' обновлена в БД (статус: processed).")
        else:
            logger.warning(
                f"Новость '{original_title[:50]}...' не найдена в БД при попытке обновления processed текста.")
    except sqlite3.Error as e:
        logger.error(f"Ошибка БД при обновлении processed текста новости '{original_title[:50]}...': {e}")
        conn.rollback()
    finally:
        conn.close()


# --- ОСНОВНАЯ ЛОГИКА ---
def process_texts_with_yacloud_sdk():
    """Основная функция обработки текстов новостей с помощью Yandex Cloud SDK."""
    logger.info("=== Начало обработки текстов новостей через Yandex Cloud SDK ===")

    # 1. Инициализируем БД
    init_db_for_processed_text(DB_NAME)

    # 2. Получаем список новостей для обработки
    fetched_news = get_fetched_news(DB_NAME)

    if not fetched_news:
        logger.info("Нет новостей для обработки (ожидающих published = 'fetched').")
        return

    logger.info(f"Найдено {len(fetched_news)} новостей для обработки.")
    processed_count = 0

    # 3. Обрабатываем каждую новость
    for news_item in fetched_news:
        original_title = news_item['title']
        full_text = news_item['full_text']

        logger.info(f"--- Обработка: {original_title[:50]}... ---")

        # --- Обработка заголовка ---
        # Передаем заголовок отдельно для перевода (без пересказа)
        title_messages = [
            {
                "role": "system",
                "text": "Ты профессиональный переводчик. Переведи следующий английский заголовок на русский язык. Сохрани все имена собственные (людей, компаний, продуктов) в оригинальном английском виде. Верни только переведенный заголовок.",
            },
            {
                "role": "user",
                "text": original_title,
            },
        ]

        try:
            sdk = YCloudML(folder_id=YC_FOLDER_ID, auth=YC_API_KEY)
            title_result = (
                sdk.models.completions("yandexgpt-lite")
                .configure(temperature=0.3, max_tokens=200)
                .run(title_messages)
            )
            # --- ИСПРАВЛЕННЫЙ ФРАГМЕНТ ---
            # Попробуем получить текст напрямую
            # Если title_result - это объект с атрибутом text
            if hasattr(title_result, 'text'):
                translated_title = title_result.text.strip()
            # Если title_result - это список или генератор
            elif hasattr(title_result, '__iter__'):
                # Берем первый элемент, если это итерируемый объект
                first_alt = next(iter(title_result))
                translated_title = first_alt.text.strip()
            else:
                # Если это какой-то другой формат, попробуем str()
                translated_title = str(title_result).strip()
            # --- КОНЕЦ ИСПРАВЛЕННОГО ФРАГМЕНТА ---
        except Exception as e:
            logger.error(f"Ошибка при переводе заголовка '{original_title[:50]}...': {e}")
            translated_title = original_title  # На случай ошибки, оставляем оригинал

        if not translated_title:
            logger.warning(f"Не удалось перевести заголовок для '{original_title[:50]}...'. Пропуск.")
            continue

        # --- Обработка полного текста ---
        if not full_text:
            logger.warning(f"Полный текст отсутствует для '{original_title[:50]}...'. Пропуск.")
            # Можно сохранить только переведенный заголовок, но по логике он нужен для 'processed'
            # continue

        # Используем объединенную функцию для перевода и пересказа
        final_processed_text = process_text_with_yacloud_sdk(full_text)
        if final_processed_text is None:  # None означает ошибку
            logger.warning(f"Не удалось обработать полный текст для '{original_title[:50]}...'. Пропуск.")
            continue
        # Пустая строка от модели будет сохранена как есть

        # --- Сохранение ---
        update_news_processed_text(DB_NAME, original_title, translated_title, final_processed_text)
        processed_count += 1

        # Небольшая задержка между запросами к API
        time.sleep(1)

    logger.info(f"=== ✔️ Обработка текстов через Yandex Cloud SDK завершена. Обработано: {processed_count} ===")


# --- ЗАПУСК ---
if __name__ == "__main__":
    # Убедитесь, что YC_FOLDER_ID и YC_API_KEY установлены как переменные окружения
    # Linux/macOS:
    # export YC_FOLDER_ID=ваш_folder_id
    # export YC_API_KEY=ваш_api_key
    # python yc_processor_sdk.py
    #
    # Windows (cmd):
    # set YC_FOLDER_ID=ваш_folder_id
    # set YC_API_KEY=ваш_api_key
    # python yc_processor_sdk.py
    #
    # Windows (PowerShell):
    # $env:YC_FOLDER_ID="ваш_folder_id"
    # $env:YC_API_KEY="ваш_api_key"
    # python yc_processor_sdk.py
    process_texts_with_yacloud_sdk()
