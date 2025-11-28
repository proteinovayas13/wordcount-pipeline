import os
import glob
import re
import shutil
import subprocess
from datetime import datetime
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def is_hadoop_available():
    """Проверяет, доступен ли Hadoop"""
    try:
        result = subprocess.run(['hadoop', 'version'], capture_output=True, text=True, shell=True)
        return result.returncode == 0
    except:
        return False

def detect_encoding(file_path):
    """Определяет кодировку файла"""
    import chardet
    try:
        with open(file_path, 'rb') as f:
            raw_data = f.read()
            result = chardet.detect(raw_data)
            encoding = result['encoding'] or 'utf-8'
            logger.info(f"Определена кодировка файла {file_path}: {encoding} (уверенность: {result['confidence']})")
            return encoding
    except Exception as e:
        logger.warning(f"Не удалось определить кодировку, используется utf-8: {e}")
        return 'utf-8'

def hadoop_wordcount(file_path, results_dir):
    """WordCount с использованием Hadoop Streaming"""
    try:
        if not is_hadoop_available():
            logger.warning("Hadoop не доступен, используется локальная обработка")
            return simple_wordcount(file_path, results_dir)
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = f"/tmp/wordcount_output_{timestamp}"
        
        # Hadoop Streaming команда
        hadoop_cmd = f"""
        hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
            -files mapper.py,reducer.py \
            -mapper "python3 mapper.py" \
            -reducer "python3 reducer.py" \
            -input {file_path} \
            -output {output_dir}
        """
        
        logger.info(f"Запуск Hadoop WordCount для: {file_path}")
        subprocess.run(hadoop_cmd, shell=True, check=True)
        
        # Копируем результат из HDFS
        result_file = f"{output_dir}/part-00000"
        local_result = os.path.join(results_dir, f"hadoop_wordcount_{timestamp}_{os.path.basename(file_path)}")
        
        subprocess.run(f"hadoop fs -copyToLocal {result_file} {local_result}", shell=True, check=True)
        logger.info(f"Hadoop WordCount завершен: {local_result}")
        return local_result
        
    except Exception as e:
        logger.error(f"Ошибка Hadoop WordCount, используется локальная обработка: {e}")
        return simple_wordcount(file_path, results_dir)

def simple_wordcount(file_path, results_dir):
    """Локальный WordCount с поддержкой разных кодировок"""
    try:
        word_count = {}
        
        # Определяем кодировку
        encoding = detect_encoding(file_path)
        
        # Пробуем прочитать файл с определенной кодировкой
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                for line in f:
                    words = re.findall(r'\w+', line.lower())
                    for word in words:
                        word_count[word] = word_count.get(word, 0) + 1
        except UnicodeDecodeError:
            # Если не удалось, пробуем другие кодировки
            for test_encoding in ['cp1251', 'iso-8859-1', 'utf-16']:
                try:
                    with open(file_path, 'r', encoding=test_encoding) as f:
                        for line in f:
                            words = re.findall(r'\w+', line.lower())
                            for word in words:
                                word_count[word] = word_count.get(word, 0) + 1
                    logger.info(f"Файл прочитан с кодировкой: {test_encoding}")
                    break
                except UnicodeDecodeError:
                    continue
            else:
                logger.error(f"Не удалось прочитать файл {file_path} ни в одной кодировке")
                return None
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(results_dir, f"wordcount_{timestamp}_{os.path.basename(file_path)}")
        
        with open(output_file, 'w', encoding='utf-8') as f:
            for word, count in sorted(word_count.items()):
                f.write(f"{word}\t{count}\n")
        
        logger.info(f"Локальный WordCount сохранен: {output_file}")
        return output_file
        
    except Exception as e:
        logger.error(f"Ошибка локального WordCount: {e}")
        return None

def process_wordcount_pipeline(use_hadoop=False):
    """Полный пайплайн WordCount"""
    
    base_dir = "C:/airflow/data"
    input_dir = os.path.join(base_dir, "input")
    processed_dir = os.path.join(base_dir, "processed")
    results_dir = os.path.join(base_dir, "results")
    upload_dir = os.path.join(base_dir, "yandex_upload")
    
    for directory in [input_dir, processed_dir, results_dir, upload_dir]:
        os.makedirs(directory, exist_ok=True)
    
    logger.info("Проверяем новые файлы...")
    
    txt_files = glob.glob(os.path.join(input_dir, "*.txt"))
    
    if not txt_files:
        logger.info("Новых файлов не найдено")
        return
    
    for file_path in txt_files:
        try:
            if not os.path.exists(file_path):
                continue
            
            logger.info(f"Обрабатываем файл: {file_path}")
            
            # Шаг 1: WordCount (Hadoop или локальный)
            if use_hadoop:
                result_file = hadoop_wordcount(file_path, results_dir)
            else:
                result_file = simple_wordcount(file_path, results_dir)
            
            if not result_file:
                logger.error(f"Не удалось обработать WordCount для файла: {file_path}")
                continue
            
            # Шаг 2: Подготовка к загрузке в Yandex Cloud
            upload_path = os.path.join(upload_dir, os.path.basename(result_file))
            shutil.copy2(result_file, upload_path)
            logger.info(f"Файл готов для Yandex Cloud: {upload_path}")
            
            # Шаг 3: Перемещение обработанного файла
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            new_path = os.path.join(processed_dir, f"{timestamp}_{os.path.basename(file_path)}")
            
            if os.path.exists(new_path):
                os.remove(new_path)
            
            os.rename(file_path, new_path)
            logger.info(f"Файл перемещен: {new_path}")
            
            logger.info(f"Файл {file_path} успешно обработан")
            
        except Exception as e:
            logger.error(f"Ошибка обработки файла {file_path}: {e}")

if __name__ == "__main__":
    import sys
    use_hadoop = "--hadoop" in sys.argv
    
    print("Запуск WordCount пайплайна...")
    print(f"Режим: {'Hadoop' if use_hadoop else 'Локальный'}")
    
    if use_hadoop and not is_hadoop_available():
        print("⚠️  Hadoop не доступен, используется локальная обработка")
        use_hadoop = False
    
    process_wordcount_pipeline(use_hadoop=use_hadoop)
    print("Пайплайн завершен")