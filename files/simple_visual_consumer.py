#!/usr/bin/env python3
"""
ПРОСТОЙ КОНСЮМЕР ДЛЯ ВИЗУАЛИЗАЦИИ РЕБАЛАНСИРОВКИ

Что демонстрирует этот скрипт:
1. Минималистичную реализацию Kafka Consumer
2. Визуальное отображение работы консюмера
3. Простоту запуска нескольких экземпляров

Особенности:
- Максимально простая реализация для демонстрации
- Показывает только факт работы консюмера
- Легко запускать в нескольких терминалах

Запуск (в нескольких терминалах):
python3 simple_visual_consumer.py --id consumer-1
python3 simple_visual_consumer.py --id consumer-2
python3 simple_visual_consumer.py --id consumer-3

Результат:
В каждом терминале будет видно как работает консюмер,
можно наблюдать за ребалансировкой при закрытии/открытии терминалов.
"""

from confluent_kafka import Consumer, KafkaError
import sys
import signal
import time

class SimpleVisualConsumer:
    def __init__(self, consumer_id, group_id="visual-demo-group"):
        self.config = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': group_id,
            'client.id': consumer_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'session.timeout.ms': 10000,
            'max.poll.interval.ms': 300000,
        }
        
        self.consumer = Consumer(self.config)
        self.consumer_id = consumer_id
        self.running = True
        
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        print(f"[{consumer_id}] Консюмер инициализирован, группа: {group_id}")
    
    def signal_handler(self, signum, frame):
        print(f"\n[{self.consumer_id}] Получен сигнал {signum}, завершение...")
        self.running = False
    
    def consume(self, topic="user-actions"):
        print(f"[{self.consumer_id}] Подписываюсь на топик {topic}")
        self.consumer.subscribe([topic])
        
        msg_count = 0
        # Таймаут без данных: если 60 секунд не пришло ни одного сообщения
        # (топик пуст или offset'ы группы уже закоммичены с прошлого запуска),
        # завершаемся с подсказкой — иначе консюмер ждал бы вечно.
        NO_DATA_TIMEOUT = 60
        no_data_since = time.time()
        
        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    if time.time() - no_data_since > NO_DATA_TIMEOUT:
                        print(f"[{self.consumer_id}] Нет сообщений дольше {NO_DATA_TIMEOUT} секунд — завершаюсь")
                        print(f"[{self.consumer_id}] Проверьте, что продюсер работает, или сбросьте offset'ы группы:")
                        print(f"[{self.consumer_id}] /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \\")
                        print(f"[{self.consumer_id}]   --group {self.config['group.id']} --reset-offsets --to-earliest --execute --topic {topic}")
                        self.running = False
                        break
                    continue
                
                # Сообщение (или событие ошибки) получено — сбрасываем таймер
                no_data_since = time.time()
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    print(f"[{self.consumer_id}] Ошибка: {msg.error()}")
                    continue
                
                msg_count += 1
                if msg_count % 10 == 0:
                    print(f"[{self.consumer_id}] Обработано сообщений: {msg_count}")
                
                # Имитация обработки
                time.sleep(0.1)
                
        except KeyboardInterrupt:
            print(f"\n[{self.consumer_id}] Прервано пользователем")
        except Exception as e:
            print(f"[{self.consumer_id}] Ошибка: {e}")
        finally:
            self.close()
    
    def close(self):
        print(f"[{self.consumer_id}] Закрываю консюмер...")
        self.consumer.close()
        print(f"[{self.consumer_id}] Консюмер закрыт")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Простой консюмер для визуализации')
    parser.add_argument('--id', required=True, help='ID консюмера')
    parser.add_argument('--topic', default='user-actions', help='Топик для подписки')
    parser.add_argument('--group', default='visual-demo-group', help='Consumer group ID')
    
    args = parser.parse_args()
    
    print(f"=== ЗАПУСК КОНСЮМЕРА {args.id} ===")
    consumer = SimpleVisualConsumer(args.id, args.group)
    consumer.consume(args.topic)
