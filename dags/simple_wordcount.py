import os
import glob
import re
import shutil
from datetime import datetime
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def process_wordcount_pipeline():
    """Полный пайплайн WordCount в одной функции"""
    
    # Пути к папкам
    base_dir = "C:/airflow/data"
    input_dir = os.path.join(base_dir, "input")
    processed_dir = os.path.join(base_dir, "processed")
    results_dir = os.path.join(base_dir, "results")
    upload_dir = os.path.join(base_dir, "yandex_upload")
    
    # Создаем папки если не существуют
    for directory in [input_dir, processed_dir, results_dir, upload_dir]:
        os.makedirs(directory, exist_ok=True)
    
    logger.info("Проверяем новые файлы...")
    
    # Ищем .txt файлы
    txt_files = glob.glob(os.path.join(input_dir, "*.txt"))
    
    if not txt_files:
        logger.info("Новых файлов не найдено")
        return
    
    for file_path in txt_files:
        try:
            logger.info(f"Обрабатываем файл: {file_path}")
            
            # Проверяем, что файл существует
            if not os.path.exists(file_path):
                logger.warning(f"Файл {file_path} не существует, пропускаем")
                continue
            
            # Шаг 1: WordCount
            word_count = {}
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    words = re.findall(r'\w+', line.lower())
                    for word in words:
                        word_count[word] = word_count.get(word, 0) + 1
            
            # Сохраняем результат
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(results_dir, f"wordcount_{timestamp}_{os.path.basename(file_path)}")
            
            with open(output_file, 'w', encoding='utf-8') as f:
                for word, count in sorted(word_count.items()):
                    f.write(f"{word}\t{count}\n")
            
            logger.info(f"WordCount сохранен в: {output_file}")
            
            # Шаг 2: Загрузка в Yandex Cloud (симуляция)
            upload_path = os.path.join(upload_dir, os.path.basename(output_file))
            shutil.copy2(output_file, upload_path)
            logger.info(f"Файл подготовлен для загрузки в Yandex Cloud: {upload_path}")
            
            # Шаг 3: Перемещение обработанного файла
            new_path = os.path.join(processed_dir, f"{timestamp}_{os.path.basename(file_path)}")
            
            # Если файл уже существует в processed, удаляем его
            if os.path.exists(new_path):
                os.remove(new_path)
            
            os.rename(file_path, new_path)
            logger.info(f"Файл перемещен в: {new_path}")
            
            logger.info(f"Файл {file_path} успешно обработан")
            
        except Exception as e:
            logger.error(f"Ошибка при обработке файла {file_path}: {e}")

if __name__ == "__main__":
    print("Запуск WordCount пайплайна...")
    process_wordcount_pipeline()
    print("Пайплайн завершен")