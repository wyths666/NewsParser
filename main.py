from rss_parser import start_parsing
from web_parser import fetch_full_texts
from yagpt_processing import process_texts_with_yacloud_sdk
from publisher import run_publisher
import logging
import asyncio
import concurrent.futures

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def run_sync_in_executor(func):

    loop = asyncio.get_running_loop()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        return await loop.run_in_executor(executor, func)

async def start_rss():
    while True:
        try:
            await run_sync_in_executor(start_parsing)
            logger.info(f'⏱️ Ожидание 30 минут до следующей обработки rss')
            await asyncio.sleep(1800)
        except Exception as e:
            logger.error(f"Ошибка в start_rss: {e}")
            await asyncio.sleep(300)

async def start_web():
    while True:
        try:
            await asyncio.sleep(60)
            await run_sync_in_executor(fetch_full_texts)
            logger.info(f'⏱️ Ожидание 30 минут до следующей обработки web')
            await asyncio.sleep(1800)
        except Exception as e:
            logger.error(f"Ошибка в start_web: {e}")
            await asyncio.sleep(300)

async def start_yagpt():
    while True:
        try:
            await asyncio.sleep(120)
            await run_sync_in_executor(process_texts_with_yacloud_sdk)
            logger.info(f'⏱️ Ожидание 35 минут до следующей обработки yagpt')
            await asyncio.sleep(2100)
        except Exception as e:
            logger.error(f"Ошибка в start_yagpt: {e}")
            await asyncio.sleep(300)

async def main():
    print("Старт")
    await asyncio.gather(
        start_rss(),
        start_web(),
        start_yagpt(),
        run_publisher()
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nРабота остановлена пользователем")