#!/usr/bin/env python3
import sys
import re

for line in sys.stdin:
    # Удаляем знаки препинания и приводим к нижнему регистру
    line = re.sub(r'[^\w\s]', '', line.strip().lower())
    words = line.split()
    for word in words:
        if word:  # проверяем, что слово не пустое
            print(f"{word}\t1")