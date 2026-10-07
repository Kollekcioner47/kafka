#!/usr/bin/env python3
"""
НАСТРОЙКА ДЕМОНСТРАЦИИ ДЛЯ ЛАБОРАТОРНОЙ 2.3

Что демонстрирует этот скрипт:
1. Подготовку окружения для всех последующих демонстраций
2. Создание тестового топика с 6 партициями и 3 репликами
3. Генерацию 5000 тестовых сообщений для экспериментов
4. Проверку доступности Kafka кластера

Концепции Kafka:
- Создание топиков через Kafka CLI
- Распределение сообщений по партициям (partitioning)
- Репликация данных для отказоустойчивости
- Основы работы Producer API

Запуск:
python3 setup_demo.py

Результат:
Будет создан топик 'user-actions' с тестовыми данными для всех последующих экспериментов.
"""

import subprocess
import json
import time
from confluent_kafka import Producer

def create_topic():
    """Создание топика для демонстрации ребалансировки"""
    print("📝 Проверяю и создаю топик 'user-actions'...")
    
    cmd = [
        '/opt/kafka/bin/kafka-topics.sh',
        '--create',
        '--bootstrap-server', 'localhost:9092',
        '--topic', 'user-actions',
        '--partitions', '6',
        '--replication-factor', '3'
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            print("✅ Топик 'user-actions' создан успешно")
            return True
        elif "already exists" in result.stderr:
            print("ℹ️  Топик 'user-actions' уже существует")
            return True
        else:
            print(f"❌ Ошибка при создании топика: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False

def produce_test_messages():
    """Создание тестовых сообщений"""
    print("\n📨 Создаю тестовые сообщения...")
    
    conf = {'bootstrap.servers': 'localhost:9092'}
    producer = Producer(conf)
    
    messages_sent = 0
    
    def delivery_report(err, msg):
        nonlocal messages_sent
        if err is not None:
            print(f'❌ Ошибка доставки: {err}')
        else:
            messages_sent += 1
            if messages_sent % 500 == 0:
                print(f"✅ Отправлено {messages_sent} сообщений...")
    
    # Отправляем 5000 тестовых сообщений
    for i in range(5000):
        message = {
            'event_id': f'event_{i}',
            'user_id': f'user_{i % 100}',
            'action': ['login', 'logout', 'purchase', 'view', 'search'][i % 5],
            'timestamp': int(time.time() * 1000),
            'data': f'Тестовое сообщение {i} для лабораторной работы'
        }
        
        producer.produce(
            'user-actions',
            key=str(i % 6),  # Распределяем по партициям
            value=json.dumps(message, ensure_ascii=False),
            callback=delivery_report
        )
        producer.poll(0)
    
    producer.flush()
    print(f"✅ Всего отправлено {messages_sent} сообщений в топик 'user-actions'")
    return True

def describe_topic():
    """Описание топика"""
    print("\n📊 Информация о топике 'user-actions':")
    
    cmd = [
        '/opt/kafka/bin/kafka-topics.sh',
        '--describe',
        '--bootstrap-server', 'localhost:9092',
        '--topic', 'user-actions'
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            print(result.stdout)
            return True
        else:
            print(f"❌ Ошибка: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False

def main():
    print("=" * 80)
    print("НАСТРОЙКА ДЕМОНСТРАЦИИ ДЛЯ ЛАБОРАТОРНОЙ 2.3")
    print("=" * 80)
    
    # Проверяем подключение к Kafka
    print("\n🔍 Проверяю подключение к Kafka...")
    try:
        cmd = ['/opt/kafka/bin/kafka-topics.sh', '--list', '--bootstrap-server', 'localhost:9092']
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
        
        if result.returncode != 0:
            print("❌ Не удалось подключиться к Kafka")
            print("   Убедитесь, что Kafka запущена: sudo systemctl start kafka")
            return
        
        print("✅ Подключение к Kafka успешно")
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")
        return
    
    # Создаем топик
    if not create_topic():
        print("\n❌ Не удалось создать топик. Выход.")
        return
    
    # Описываем топик
    describe_topic()
    
    # Создаем тестовые сообщения
    print("\n" + "=" * 80)
    choice = input("Создать тестовые сообщения (5000 сообщений)? (y/n): ").strip().lower()
    
    if choice == 'y':
        if produce_test_messages():
            print("\n✅ Настройка завершена успешно!")
            print("\nТеперь вы можете запускать лабораторные скрипты:")
            print("  1. python3 basic_consumer.py")
            print("  2. python3 manual_commit_consumer.py --id consumer-1")
            print("  3. python3 consumer_group_demo.py")
            print("  4. python3 error_handling_consumer.py --produce --messages 30")
            print("  5. python3 rebalance_test.py")
        else:
            print("❌ Не удалось создать тестовые сообщения")
    else:
        print("ℹ️  Пропускаем создание тестовых сообщений")
        print("\n⚠️  ВНИМАНИЕ: Консюмеры могут не отображать информацию,")
        print("   если в топике нет сообщений")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n👋 Настройка прервана")
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
