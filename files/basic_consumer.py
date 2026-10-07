#!/usr/bin/env python3
"""
БАЗОВЫЙ КОНСЮМЕР KAFKA С АВТОМАТИЧЕСКИМ КОММИТОМ

Что демонстрирует этот скрипт:
1. Простейшую реализацию Kafka Consumer
2. Автоматический коммит offset (enable.auto.commit = True)
3. Основы подписки на топик и потребления сообщений
4. Graceful shutdown при получении сигналов (SIGINT, SIGTERM)

Концепции Kafka:
- Consumer Groups и group.id
- Auto offset commit (автоматическое подтверждение обработки)
- Auto offset reset (earliest/latest)
- Потребление сообщений с обработкой ошибок

Особенности:
- Простота использования, но риск потери данных
- Автокоммит каждые 5 секунд
- Нет гарантии exactly-once обработки
- Подходит для не критичных данных

Запуск:
python3 basic_consumer.py

Результат:
Консюмер обработает 100 сообщений, покажет статистику и завершится.
Вы увидите как сообщения распределяются по партициям.
"""

from confluent_kafka import Consumer, KafkaError
import json
import time
import signal
import sys
from rich.console import Console
from rich.table import Table
from rich.progress import Progress
from datetime import datetime
import subprocess

console = Console()

def check_topic_exists(topic="user-actions"):
    """Проверка существования топика"""
    try:
        cmd = ['/opt/kafka/bin/kafka-topics.sh', '--describe', 
               '--bootstrap-server', 'localhost:9092', '--topic', topic]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
        
        if result.returncode != 0 or f"Topic: {topic}" not in result.stdout:
            return False
        return True
    except Exception:
        return False

class BasicConsumer:
    def __init__(self, bootstrap_servers, group_id, topic):
        """
        Инициализация базового консюмера
        
        Args:
            bootstrap_servers: строка с адресами брокеров
            group_id: идентификатор группы потребителей
            topic: топик для подписки
        """
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',  # Начинаем с начала топика
            'enable.auto.commit': True,       # Автоматический коммит offset
            'auto.commit.interval.ms': 5000,  # Коммитим каждые 5 секунд
            'session.timeout.ms': 10000,      # Таймаут сессии
            'max.poll.interval.ms': 300000,   # Максимальное время обработки
        }
        
        self.consumer = Consumer(self.config)
        self.topic = topic
        self.running = True
        
        # Статистика
        self.stats = {
            'messages_processed': 0,
            'errors': 0,
            'start_time': None,
            'last_commit': time.time(),
            'offsets': {}
        }
        
        # Обработка сигналов для graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def signal_handler(self, signum, frame):
        """Обработчик сигналов для graceful shutdown"""
        console.print(f"\n[yellow]⚠️  Получен сигнал {signum}, завершаем работу...[/yellow]")
        self.running = False
    
    def process_message(self, message):
        """
        Обработка одного сообщения
        
        Args:
            message: объект сообщения Kafka
        """
        try:
            # Декодируем сообщение
            value = message.value().decode('utf-8')
            data = json.loads(value)
            
            # Простая обработка
            console.print(f"\n[cyan]📨 Получено сообщение:[/cyan]")
            console.print(f"  Partition: {message.partition()}")
            console.print(f"  Offset: {message.offset()}")
            console.print(f"  Key: {message.key().decode('utf-8') if message.key() else 'None'}")
            console.print(f"  Event ID: {data.get('event_id', 'N/A')}")
            console.print(f"  User ID: {data.get('user_id', 'N/A')}")
            console.print(f"  Action: {data.get('action', 'N/A')}")
            console.print(f"  Timestamp: {data.get('timestamp', 'N/A')}")
            
            # Имитация обработки
            time.sleep(0.01)
            
            self.stats['messages_processed'] += 1
            return True
            
        except json.JSONDecodeError as e:
            console.print(f"[red]❌ Ошибка декодирования JSON: {e}[/red]")
            self.stats['errors'] += 1
            return False
        except Exception as e:
            console.print(f"[red]❌ Ошибка обработки: {e}[/red]")
            self.stats['errors'] += 1
            return False
    
    def consume(self, max_messages=None):
        """
        Основной цикл потребления сообщений
        
        Args:
            max_messages: максимальное количество сообщений для обработки
        """
        console.print(f"[bold green]🚀 Запуск базового консюмера[/bold green]")
        console.print(f"👥 Group ID: {self.config['group.id']}")
        console.print(f"📭 Топик: {self.topic}")
        console.print(f"🤖 Автокоммит: {'Включен' if self.config['enable.auto.commit'] else 'Выключен'}")
        
        self.consumer.subscribe([self.topic])
        self.stats['start_time'] = time.time()
        
        message_count = 0
        
        # Таймаут без данных: если 60 секунд не пришло НИ ОДНОГО сообщения
        # (топик пуст или offset'ы группы уже закоммичены с прошлого запуска),
        # выходим с подсказкой. Иначе консюмер крутился бы вечно: выход в этом
        # скрипте только по max_messages, а enable.partition.eof не включён,
        # поэтому ветка «конец партиции» ниже не срабатывает.
        NO_DATA_TIMEOUT = 60
        no_data_since = time.time()
        
        with Progress() as progress:
            task = progress.add_task("Потребление сообщений...", total=max_messages)
            
            while self.running:
                if max_messages and message_count >= max_messages:
                    console.print(f"\n[yellow]⚠️  Достигнут лимит {max_messages} сообщений[/yellow]")
                    break
                
                try:
                    # Получаем сообщение с таймаутом 1 секунда
                    msg = self.consumer.poll(timeout=1.0)
                    
                    if msg is None:
                        # Сообщений нет — проверяем, не пора ли выйти по таймауту
                        if time.time() - no_data_since > NO_DATA_TIMEOUT:
                            console.print(f"\n[yellow]⚠️  Нет сообщений дольше {NO_DATA_TIMEOUT} секунд — завершаюсь[/yellow]")
                            console.print("[yellow]📌 Проверьте, что продюсер работает, или сбросьте offset'ы группы:[/yellow]")
                            console.print("[yellow]   /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \\[/yellow]")
                            console.print(f"[yellow]     --group {self.config['group.id']} --reset-offsets --to-earliest --execute --topic {self.topic}[/yellow]")
                            console.print("[yellow]   либо пересоздайте данные: python3 setup_demo.py[/yellow]")
                            self.running = False
                            break
                        continue
                    
                    # Сообщение (или событие ошибки) получено — сбрасываем таймер
                    no_data_since = time.time()
                    
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            console.print(f"[yellow]📭 Достигнут конец партиции {msg.partition()}[/yellow]")
                        else:
                            console.print(f"[red]❌ Ошибка Kafka: {msg.error()}[/red]")
                        continue
                    
                    # Обрабатываем сообщение
                    if self.process_message(msg):
                        message_count += 1
                        
                        # Обновляем прогресс
                        if max_messages:
                            progress.update(task, advance=1)
                        
                        # Периодически показываем статистику
                        if message_count % 100 == 0:
                            self.show_stats()
                    
                except KeyboardInterrupt:
                    console.print("\n[yellow]⚠️  Прервано пользователем[/yellow]")
                    break
                except Exception as e:
                    console.print(f"[red]❌ Неожиданная ошибка: {e}[/red]")
                    self.stats['errors'] += 1
        
        # Завершаем работу
        self.close()
    
    def show_stats(self):
        """Отображение статистики"""
        current_time = time.time()
        duration = current_time - self.stats['start_time']
        
        if duration > 0:
            rate = self.stats['messages_processed'] / duration
        else:
            rate = 0
        
        console.print(f"\n[bold]📊 Текущая статистика:[/bold]")
        console.print(f"  Обработано сообщений: {self.stats['messages_processed']}")
        console.print(f"  Ошибок: {self.stats['errors']}")
        console.print(f"  Скорость обработки: {rate:.2f} сообщ/сек")
        console.print(f"  Время работы: {duration:.2f} сек")
    
    def close(self):
        """Корректное закрытие консюмера"""
        console.print("\n[yellow]🔒 Закрытие консюмера...[/yellow]")
        
        # Выводим итоговую статистику
        console.print("\n[bold green]📈 ИТОГОВАЯ СТАТИСТИКА:[/bold green]")
        self.show_stats()
        
        # Закрываем консюмер
        self.consumer.close()
        console.print("[green]✅ Консюмер корректно закрыт[/green]")

if __name__ == "__main__":
    # Конфигурация
    BOOTSTRAP_SERVERS = "localhost:9092"
    GROUP_ID = "basic-consumer-group"
    TOPIC = "user-actions"
    
    # Проверяем существование топика
    if not check_topic_exists(TOPIC):
        console.print("[red]❌ Топик 'user-actions' не найден![/red]")
        console.print("[yellow]📌 Запустите сначала подготовку окружения:[/yellow]")
        console.print("[yellow]   python3 setup_demo.py[/yellow]")
        sys.exit(1)
    
    # Создаем и запускаем консюмер
    consumer = BasicConsumer(BOOTSTRAP_SERVERS, GROUP_ID, TOPIC)
    
    # Обрабатываем 100 сообщений для демонстрации
    consumer.consume(max_messages=100)
