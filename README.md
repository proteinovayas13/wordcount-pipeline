Основные функции:

-Таск проверки папки - simple_wordcount.py периодически проверяет input/ на новые .txt файлы.

-Таск WordCount - выполняет подсчет слов (локально или через Hadoop).

-Таск загрузки в Yandex Cloud - файлы подготавливаются в yandex_upload/.

-Таск перемещения файлов - исходные файлы перемещаются из input/ в processed/.

Дополнительные функции:

-Hadoop Streaming - готовые mapper.py и reducer.py.

-Автоопределение кодировок - работа с любыми текстовыми файлами.

-Умное переключение - автоматически использует локальную обработку если Hadoop недоступен.

-Подробное логирование - отслеживание всех этапов.

Основные файлы:

-simple_wordcount.py - главный пайплайн.

-scheduler.py - планировщик для периодического запуска.
-monitor.py - мониторинг состояния папок.

    Hadoop MapReduce:
    
-mapper.py - mapper для Hadoop Streaming.

-reducer.py - reducer для Hadoop Streaming.

Архитектура проекта

Пользователь
    ↓
[input/] ← .txt файлы
    ↓
simple_wordcount.py (пайплайн)

    ├── WordCount (локальный/Hadoop)
    
    ├── Сохранение результатов [results/]  
    
    ├── Подготовка к Yandex Cloud [yandex_upload/]
    
    └── Перемещение файлов [input/] → [processed/]
        ↓
scheduler.py ← Периодический запуск
