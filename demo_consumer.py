#!/usr/bin/env python3
"""
Демонстрационный консюмер для визуализации ребалансировки
Работает бесконечно, пока не получит сигнал на завершение
"""

from confluent_kafka import Consumer, KafkaError
import json
import time
import signal
import sys

class DemoConsumer:
    def __init__(self, consumer_id, group_id="visual-demo-group"):
        self.config = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': group_id,
            'client.id': consumer_id,  # Для отображения в consumer group
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'session.timeout.ms': 10000,
            'max.poll.interval.ms': 300000,
        }
        
        self.consumer = Consumer(self.config)
        self.consumer_id = consumer_id
        self.running = True
        self.messages_processed = 0
        
        # Обработка сигналов
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        print(f"[{consumer_id}] Консюмер инициализирован")
    
    def signal_handler(self, signum, frame):
        print(f"\n[{self.consumer_id}] Получен сигнал {signum}, завершение...")
        self.running = False
    
    def consume(self, topic="user-actions"):
        """Бесконечное потребление сообщений"""
        print(f"[{self.consumer_id}] Подписываюсь на топик {topic}")
        self.consumer.subscribe([topic])
        
        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print(f"[{self.consumer_id}] Конец партиции {msg.partition()}")
                    continue
                
                # Простая обработка
                try:
                    value = msg.value().decode('utf-8')
                    data = json.loads(value)
                    self.messages_processed += 1
                    
                    if self.messages_processed % 10 == 0:
                        print(f"[{self.consumer_id}] Обработано {self.messages_processed} сообщений")
                    
                except Exception as e:
                    print(f"[{self.consumer_id}] Ошибка обработки: {e}")
                
                # Имитация работы
                time.sleep(0.1)
                
                # Коммитим каждые 5 сообщений
                if self.messages_processed % 5 == 0:
                    self.consumer.commit(asynchronous=False)
        
        except KeyboardInterrupt:
            print(f"\n[{self.consumer_id}] Прервано пользователем")
        except Exception as e:
            print(f"[{self.consumer_id}] Ошибка: {e}")
        finally:
            self.close()
    
    def close(self):
        """Корректное завершение"""
        print(f"[{self.consumer_id}] Закрываю консюмер...")
        self.consumer.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Демо-консюмер для визуализации ребалансировки')
    parser.add_argument('--id', required=True, help='ID консюмера')
    parser.add_argument('--topic', default='user-actions', help='Топик для подписки')
    parser.add_argument('--group', default='visual-demo-group', help='Consumer group ID')
    
    args = parser.parse_args()
    
    print(f"=== ЗАПУСК ДЕМО-КОНСЮМЕРА {args.id} ===")
    print(f"Group: {args.group}")
    print(f"Topic: {args.topic}")
    print("Для остановки нажмите Ctrl+C")
    print("="*50)
    
    consumer = DemoConsumer(args.id, args.group)
    consumer.consume(args.topic)
