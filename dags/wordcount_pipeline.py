from datetime import datetime, timedelta
import os
import glob
import re
from airflow import DAG
from airflow.operators.python import PythonOperator

def check_new_files(**context):
    """Проверяет папку на наличие новых .txt файлов"""
    input_dir = "C:/airflow/data/input"
    print(f"Проверяем папку: {input_dir}")
    
    txt_files = glob.glob(os.path.join(input_dir, "*.txt"))
    print(f"Найдены файлы: {txt_files}")
    
    if txt_files:
        # Берем первый найденный файл
        file_path = txt_files[0]
        context['ti'].xcom_push(key='file_path', value=file_path)
        print(f"Обрабатываем файл: {file_path}")
        return file_path
    
    print("Новых файлов не найдено")
    return None

def wordcount_file(**context):
    """Выполняет WordCount для файла"""
    file_path = context['ti'].xcom_pull(task_ids='check_files_task', key='file_path')
    
    if not file_path or not os.path.exists(file_path):
        print("Файл не найден для обработки")
        return None
    
    print(f"Выполняем WordCount для файла: {file_path}")
    
    # WordCount логика
    word_count = {}
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                words = re.findall(r'\w+', line.lower())
                for word in words:
                    word_count[word] = word_count.get(word, 0) + 1
    except Exception as e:
        print(f"Ошибка при чтении файла: {e}")
        return None
    
    # Сохраняем результат
    output_file = f"C:/airflow/data/results/wordcount_{os.path.basename(file_path)}"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            for word, count in sorted(word_count.items()):
                f.write(f"{word}\t{count}\n")
        
        print(f"Результат сохранен в: {output_file}")
        return output_file
    except Exception as e:
        print(f"Ошибка при сохранении результата: {e}")
        return None

def upload_to_yandex(**context):
    """Загружает файл в Yandex Cloud (заглушка)"""
    result_file = context['ti'].xcom_pull(task_ids='wordcount_task')
    
    if not result_file:
        print("Нет файла для загрузки")
        return None
    
    print(f"Загружаем файл {result_file} в Yandex Cloud")
    # Здесь будет логика загрузки через Yandex Cloud SDK
    # Для демонстрации просто копируем файл
    try:
        import shutil
        upload_dir = "C:/airflow/data/yandex_upload"
        os.makedirs(upload_dir, exist_ok=True)
        shutil.copy2(result_file, upload_dir)
        print(f"Файл скопирован в папку загрузки: {upload_dir}")
    except Exception as e:
        print(f"Ошибка при копировании файла: {e}")
    
    return f"uploaded_{os.path.basename(result_file)}"

def move_processed_file(**context):
    """Перемещает обработанный файл"""
    original_file = context['ti'].xcom_pull(task_ids='check_files_task', key='file_path')
    
    if not original_file or not os.path.exists(original_file):
        print("Исходный файл не найден для перемещения")
        return
    
    processed_dir = "C:/airflow/data/processed"
    os.makedirs(processed_dir, exist_ok=True)
    new_path = os.path.join(processed_dir, os.path.basename(original_file))
    
    try:
        os.rename(original_file, new_path)
        print(f"Файл перемещен: {original_file} -> {new_path}")
    except Exception as e:
        print(f"Ошибка при перемещении файла: {e}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'simple_wordcount_pipeline',
    default_args=default_args,
    description='Простой WordCount пайплайн для текстовых файлов',
    schedule_interval=timedelta(minutes=5),  # Проверка каждые 5 минут
    catchup=False,
    tags=['wordcount', 'yandex'],
) as dag:

    check_files = PythonOperator(
        task_id='check_files_task',
        python_callable=check_new_files,
        provide_context=True,
    )

    wordcount_task = PythonOperator(
        task_id='wordcount_task',
        python_callable=wordcount_file,
        provide_context=True,
    )

    upload_task = PythonOperator(
        task_id='upload_to_yandex_task',
        python_callable=upload_to_yandex,
        provide_context=True,
    )

    move_task = PythonOperator(
        task_id='move_file_task',
        python_callable=move_processed_file,
        provide_context=True,
    )

    check_files >> wordcount_task >> upload_task >> move_task