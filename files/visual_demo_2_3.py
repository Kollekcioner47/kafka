#!/usr/bin/env python3
"""
ВИЗУАЛЬНАЯ ДЕМОНСТРАЦИЯ РЕБАЛАНСИРОВКИ ДЛЯ ЛАБОРАТОРНОЙ 2.3

Что демонстрирует этот скрипт:
1. Пошаговую демонстрацию ребалансировки
2. Запуск и остановку консюмеров по сценарию
3. Проверку состояния consumer group после каждого шага
4. Наглядное представление распределения партиций

Сценарий демонстрации:
1. Запуск одного консюмера (получает все 6 партиций)
2. Запуск второго консюмера (ребалансировка, по 3 партиции каждому)
3. Остановка второго консюмера (ребалансировка, все партиции первому)
4. Завершение демонстрации

Запуск:
python3 visual_demo_2_3.py

Результат:
Пошаговая демонстрация с подсказками и проверкой состояния.
Идеально для лекций и презентаций.
"""

import subprocess
import time
import threading

def run_consumer(consumer_id):
    """Запуск консюмера"""
    cmd = ['python3', 'simple_visual_consumer.py', '--id', consumer_id]
    
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1
    )
    
    def read_output(pipe, name):
        for line in pipe:
            if line.strip():
                print(f"[{name}] {line.strip()}")
    
    threading.Thread(target=read_output, args=(process.stdout, consumer_id), daemon=True).start()
    threading.Thread(target=read_output, args=(process.stderr, consumer_id), daemon=True).start()
    
    return process

def check_consumer_group():
    """Проверка состояния consumer group"""
    cmd = [
        '/opt/kafka/bin/kafka-consumer-groups.sh',
        '--bootstrap-server', 'localhost:9092',
        '--group', 'visual-demo-group',
        '--describe'
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
    return result.stdout if result.returncode == 0 else result.stderr

def main():
    print("ВИЗУАЛЬНАЯ ДЕМОНСТРАЦИЯ РЕБАЛАНСИРОВКИ")
    print("=" * 60)
    
    # Проверяем топик
    print("🔍 Проверяю топик 'user-actions'...")
    try:
        cmd = ['/opt/kafka/bin/kafka-topics.sh', '--describe', 
               '--bootstrap-server', 'localhost:9092', '--topic', 'user-actions']
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
        
        if result.returncode != 0 or "Topic: user-actions" not in result.stdout:
            print("❌ Топик 'user-actions' не найден!")
            print("\n📌 Вам нужно сначала настроить демонстрацию:")
            print("   1. Запустите: python3 setup_demo.py")
            print("   2. Создайте топик и тестовые данные")
            print("   3. Затем запустите эту демонстрацию снова")
            return
    except Exception as e:
        print(f"❌ Ошибка при проверке: {e}")
        return
    
    processes = {}
    
    input("Нажмите Enter для запуска первого консюмера...")
    processes['consumer-1'] = run_consumer('consumer-1')
    time.sleep(5)
    print("\nСостояние после запуска consumer-1:")
    print(check_consumer_group())
    
    input("\nНажмите Enter для запуска второго консюмера...")
    processes['consumer-2'] = run_consumer('consumer-2')
    time.sleep(5)
    print("\nСостояние после запуска consumer-2 (ребалансировка):")
    print(check_consumer_group())
    
    input("\nНажмите Enter для остановки consumer-2...")
    processes['consumer-2'].terminate()
    processes['consumer-2'].wait()
    del processes['consumer-2']
    time.sleep(5)
    print("\nСостояние после остановки consumer-2 (ребалансировка):")
    print(check_consumer_group())
    
    input("\nНажмите Enter для завершения демонстрации...")
    for name, process in processes.items():
        process.terminate()
        process.wait()
    
    print("\n✅ Демонстрация завершена!")

if __name__ == "__main__":
    main()
