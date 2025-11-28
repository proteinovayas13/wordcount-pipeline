import os
import time
from datetime import datetime

def monitor_folders():
    base_dir = "C:/airflow/data"
    folders = ["input", "processed", "results", "yandex_upload"]
    
    while True:
        print(f"\n=== {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")
        for folder in folders:
            folder_path = os.path.join(base_dir, folder)
            files = os.listdir(folder_path) if os.path.exists(folder_path) else []
            print(f"{folder.upper()}: {len(files)} файлов")
            for file in files:
                file_path = os.path.join(folder_path, file)
                size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
                print(f"  - {file} ({size} bytes)")
        time.sleep(10)

if __name__ == "__main__":
    monitor_folders()