import time
import schedule
from simple_wordcount import process_wordcount_pipeline
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_pipeline():
    """Запускает пайплайн"""
    logger.info("Запуск WordCount пайплайна...")
    process_wordcount_pipeline()
    logger.info("Пайплайн завершен")

# Настраиваем расписание
schedule.every(2).minutes.do(run_pipeline)

if __name__ == "__main__":
    logger.info("Планировщик запущен. Проверка каждые 2 минуты...")
    logger.info("Нажмите Ctrl+C для остановки")
    
    # Запускаем сразу при старте
    run_pipeline()
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Планировщик остановлен")