#!/usr/bin/env python3
"""
КОНСЮМЕР С РУЧНЫМ УПРАВЛЕНИЕМ OFFSET (MANUAL COMMIT)

Что демонстрирует этот скрипт:
1. Ручной коммит offset (enable.auto.commit = False) — приложение само решает,
   когда подтвердить обработку
2. Обработку сообщений с повторными попытками (retry logic)
3. Callback'и для ребалансировки (on_assign, on_revoke, on_lost)
4. Dead Letter Queue (DLQ) для неудачных сообщений
5. Защиту разделяемого состояния блокировкой (threading.Lock)

Концепции Kafka:
- Ручной коммит offset (enable.auto.commit = False)
- Consumer rebalance callbacks
- Гарантии доставки: at-least-once + DLQ (НЕ exactly-once!)
- Partition assignment strategies
- Обработка ошибок с экспоненциальной задержкой

ВАЖНО про гарантии доставки (честная формулировка):
- Этот скрипт НЕ реализует exactly-once. Реальная семантика — at-least-once + DLQ:
  * при крэше МЕЖДУ обработкой сообщения и коммитом offset сообщение будет
    перечитано после рестарта (возможны дубликаты) — это at-least-once;
  * сообщения, упавшие после всех попыток, уходят в DLQ-файл, а их offset
    молча «перепрыгивается» последующим коммитом — для таких сообщений
    фактически действует at-most-once (после рестарта они не перечитываются);
  * настоящий exactly-once требует транзакционного продюсера/консюмера
    (Kafka transactions) и в этой лабораторной не реализуется.

Особенности:
- Требует явного вызова consumer.commit()
- Сложнее в реализации, но даёт контроль над моментом подтверждения
- Подходит для задач, где важна доставка без потерь, но допустимы редкие дубликаты

Запуск:
python3 manual_commit_consumer.py --id consumer-1 --messages 50

Результат:
Консюмер обработает указанное количество сообщений с ручным коммитом,
покажет статистику по успешным/неудачным обработкам и создаст DLQ файл.
"""

from confluent_kafka import Consumer, KafkaError, TopicPartition
import json
import time
import signal
import sys
from rich.console import Console
from rich.table import Table
from collections import defaultdict
import threading
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

class ManualCommitConsumer:
    def __init__(self, bootstrap_servers, group_id, topic, consumer_id="consumer-1"):
        """
        Консюмер с ручным коммитом offset
        
        Args:
            bootstrap_servers: строка с адресами брокеров
            group_id: идентификатор группы потребителей
            topic: топик для подписки
            consumer_id: уникальный идентификатор консюмера
        """
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,  # ВЫКЛЮЧАЕМ автокоммит!
            'session.timeout.ms': 10000,
            'max.poll.interval.ms': 300000,
            'partition.assignment.strategy': 'roundrobin',
        }
        
        self.consumer = Consumer(self.config)
        self.topic = topic
        self.consumer_id = consumer_id
        self.running = True
        
        # Статистика
        self.stats = {
            'messages_processed': 0,
            'messages_committed': 0,
            'messages_failed': 0,
            'commits_successful': 0,
            'commits_failed': 0,
            'rebalances': 0,
            'start_time': time.time(),
        }
        
        # Храним offset'ы для ручного коммита
        self.pending_offsets = defaultdict(dict)  # partition -> {offset: message_data}
        self.committed_offsets = {}  # partition -> последний коммитнутый offset
        
        # Блокировка для thread-safe операций
        self.lock = threading.Lock()
        
        # Обработка сигналов
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        # ВАЖНО: confluent-kafka Consumer НЕ потокобезопасен — коммит нельзя
        # вызывать из фонового threading.Timer, пока главный поток в poll().
        # Поэтому периодический коммит выполняется ЗДЕСЬ ЖЕ, в главном цикле
        # consume(): по расписанию (каждые commit_interval_sec секунд) или
        # после накопления достаточного числа сообщений.
        self.last_commit_time = time.time()
        self.commit_interval_sec = 10
    
    def signal_handler(self, signum, frame):
        """Обработчик сигналов"""
        console.print(f"\n[yellow]⚠️  [{self.consumer_id}] Получен сигнал {signum}[/yellow]")
        self.running = False
    
    def on_assign(self, consumer, partitions):
        """Callback при назначении партиций"""
        console.print(f"\n[green]✅ [{self.consumer_id}] Назначены партиции:[/green]")
        for p in partitions:
            console.print(f"  Partition {p.partition} (offset: {p.offset})")
            self.committed_offsets[p.partition] = p.offset
        
        self.stats['rebalances'] += 1
    
    def on_revoke(self, consumer, partitions):
        """Callback при отзыве партиций (перед ребалансировкой)"""
        console.print(f"\n[yellow]🔄 [{self.consumer_id}] Ребалансировка! Отзыв партиций...[/yellow]")
        
        # КОММИТИМ ВСЕ ОЖИДАЮЩИЕ OFFSET'Ы перед ребалансировкой
        self.commit_offsets(force=True)
        
        for p in partitions:
            console.print(f"  Partition {p.partition}")
    
    def on_lost(self, consumer, partitions):
        """Callback при потере партиций (после таймаута)"""
        console.print(f"\n[red]💥 [{self.consumer_id}] Потеряны партиции![/red]")
        for p in partitions:
            console.print(f"  Partition {p.partition}")
    
    def process_message_with_retry(self, msg, max_retries=3):
        """
        Обработка сообщения с повторными попытками
        
        Args:
            msg: сообщение Kafka
            max_retries: максимальное количество попыток
        """
        for attempt in range(max_retries):
            try:
                # Декодируем и обрабатываем
                value = msg.value().decode('utf-8')
                data = json.loads(value)
                
                # Имитация обработки: ~5% сообщений — «отравленные» (hash(event_id)
                # стабилен в рамках процесса), поэтому они падают на КАЖДОЙ попытке
                # и после исчерпания ретраев уходят в DLQ-файл. Остальные 95%
                # обрабатываются с первой попытки.
                if hash(data.get('event_id', '')) % 100 < 5:
                    raise Exception("Симулированная ошибка обработки")
                
                # Успешная обработка
                console.print(f"\n[green]✅ [{self.consumer_id}] Обработано:[/green]")
                console.print(f"  Partition: {msg.partition()}, Offset: {msg.offset()}")
                console.print(f"  User: {data.get('user_id', 'N/A')[:8]}..., Action: {data.get('action', 'N/A')}")
                
                # Сохраняем для коммита
                with self.lock:
                    self.pending_offsets[msg.partition()][msg.offset()] = {
                        'data': data,
                        'processed_at': time.time()
                    }
                
                self.stats['messages_processed'] += 1
                return True
                
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # Экспоненциальная задержка
                    console.print(f"[yellow]🔄 [{self.consumer_id}] Попытка {attempt+1}/{max_retries} не удалась: {e}")
                    console.print(f"[yellow]   Ждем {wait_time} секунд...[/yellow]")
                    time.sleep(wait_time)
                else:
                    console.print(f"[red]❌ [{self.consumer_id}] Все попытки не удались для offset {msg.offset()}[/red]")
                    self.stats['messages_failed'] += 1
                    
                    # Можно отправить в Dead Letter Queue
                    self.send_to_dlq(msg, str(e))
                    return False
        
        return False
    
    def send_to_dlq(self, msg, error):
        """Отправка неудачного сообщения в Dead Letter Queue"""
        dlq_data = {
            'original_message': msg.value().decode('utf-8') if msg.value() else None,
            'error': error,
            'partition': msg.partition(),
            'offset': msg.offset(),
            'timestamp': time.time(),
            'consumer_id': self.consumer_id,
        }
        
        # В реальном приложении здесь была бы отправка в DLQ топик
        console.print(f"[magenta]📭 [{self.consumer_id}] Отправлено в DLQ: offset {msg.offset()}[/magenta]")
        
        # Для демонстрации сохраняем в файл
        with open(f"dlq_{self.consumer_id}.json", "a") as f:
            f.write(json.dumps(dlq_data) + "\n")
    
    def commit_offsets(self, force=False):
        """
        Ручной коммит offset'ов
        
        Args:
            force: коммитить все pending offsets, даже если их мало
        """
        with self.lock:
            if not self.pending_offsets:
                return
            
            offsets_to_commit = []
            total_messages = 0
            
            # Собираем offsets для коммита
            for partition, offsets in self.pending_offsets.items():
                if not offsets:
                    continue
                
                # Находим максимальный offset для каждой партиции.
                # NB: сообщения, упавшие после всех попыток, в pending НЕ попадают
                # (они ушли в DLQ-файл), поэтому их offset будет «перепрыгнут»
                # этим коммитом — для них семантика at-most-once (см. докстринг).
                max_offset = max(offsets.keys())
                offsets_to_commit.append(
                    TopicPartition(self.topic, partition, max_offset + 1)
                )
                
                total_messages += len(offsets)
            
            # Коммитим только если накопилось достаточно сообщений или force=True
            if force or total_messages >= 10:  # Коммитим каждые 10 сообщений
                try:
                    self.consumer.commit(offsets=offsets_to_commit, asynchronous=False)
                    
                    console.print(f"\n[green]💾 [{self.consumer_id}] Закоммичены offsets:[/green]")
                    for tp in offsets_to_commit:
                        console.print(f"  Partition {tp.partition} -> offset {tp.offset}")
                        self.committed_offsets[tp.partition] = tp.offset
                    
                    self.stats['commits_successful'] += 1
                    self.stats['messages_committed'] += total_messages
                    
                    # Очищаем pending offsets
                    self.pending_offsets.clear()
                    
                    # Обновляем время последнего коммита (для коммита по
                    # расписанию из главного цикла consume)
                    self.last_commit_time = time.time()
                    
                except Exception as e:
                    console.print(f"[red]❌ [{self.consumer_id}] Ошибка коммита: {e}[/red]")
                    self.stats['commits_failed'] += 1
    
    def consume(self, max_messages=None):
        """
        Основной цикл потребления
        
        Args:
            max_messages: максимальное количество сообщений
        """
        console.print(f"\n[bold green]🚀 [{self.consumer_id}] Запуск консюмера с ручным коммитом[/bold green]")
        console.print(f"👥 Group ID: {self.config['group.id']}")
        console.print(f"📭 Топик: {self.topic}")
        console.print(f"🔧 Стратегия назначения: {self.config['partition.assignment.strategy']}")
        
        # Настраиваем callback'и
        self.consumer.subscribe(
            [self.topic],
            on_assign=self.on_assign,
            on_revoke=self.on_revoke,
            on_lost=self.on_lost
        )
        
        message_count = 0
        
        # Таймаут без данных: выходим, если NO_DATA_TIMEOUT секунд не пришло
        # ни одного сообщения (иначе при пустом или полностью потреблённом
        # топике цикл крутился бы вечно — выход только по max_messages).
        NO_DATA_TIMEOUT = 60
        no_data_since = time.time()
        
        try:
            while self.running and (max_messages is None or message_count < max_messages):
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    # Коммит по расписанию: время истекло, а новых сообщений нет.
                    # Раньше это делал фоновый threading.Timer, но Consumer не
                    # потокобезопасен — коммитим здесь же, в главном цикле.
                    if time.time() - self.last_commit_time >= self.commit_interval_sec:
                        self.commit_offsets()
                    
                    # Нет данных дольше таймаута — выходим с подсказкой
                    if time.time() - no_data_since > NO_DATA_TIMEOUT:
                        console.print(f"\n[yellow]⚠️  [{self.consumer_id}] Нет сообщений дольше {NO_DATA_TIMEOUT} секунд — завершаюсь[/yellow]")
                        console.print("[yellow]📌 Проверьте, что продюсер работает, или сбросьте offset'ы группы:[/yellow]")
                        console.print("[yellow]   /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \\[/yellow]")
                        console.print(f"[yellow]     --group {self.config['group.id']} --reset-offsets --to-earliest --execute --topic {self.topic}[/yellow]")
                        self.running = False
                        break
                    continue
                
                # Сообщение (или событие ошибки) получено — сбрасываем таймер
                no_data_since = time.time()
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        console.print(f"[yellow]📭 [{self.consumer_id}] Конец партиции {msg.partition()}[/yellow]")
                    elif msg.error().code() == KafkaError._UNKNOWN_TOPIC_OR_PART:
                        console.print(f"[red]❌ [{self.consumer_id}] Топик или партиция не найдены[/red]")
                        break
                    else:
                        console.print(f"[red]❌ [{self.consumer_id}] Ошибка Kafka: {msg.error()}[/red]")
                    continue
                
                # Обрабатываем сообщение
                if self.process_message_with_retry(msg):
                    message_count += 1
                    
                    # Коммитим по расписанию: каждые 10 сообщений ИЛИ раз в
                    # commit_interval_sec секунд. Время проверяем прямо в потоке
                    # консюмера — без фоновых таймеров.
                    if (message_count % 10 == 0) or \
                       (time.time() - self.last_commit_time >= self.commit_interval_sec):
                        self.commit_offsets()
                    
                    # Показываем статистику каждые 50 сообщений
                    if message_count % 50 == 0:
                        self.show_stats()
        
        except KeyboardInterrupt:
            console.print(f"\n[yellow]⚠️  [{self.consumer_id}] Прервано пользователем[/yellow]")
        except Exception as e:
            console.print(f"[red]❌ [{self.consumer_id}] Критическая ошибка: {e}[/red]")
        finally:
            self.close()
    
    def show_stats(self):
        """Показать статистику"""
        duration = time.time() - self.stats['start_time']
        
        table = Table(title=f"📊 Статистика {self.consumer_id}")
        table.add_column("Метрика", style="cyan")
        table.add_column("Значение", style="green")
        
        table.add_row("Обработано сообщений", str(self.stats['messages_processed']))
        table.add_row("Закоммичено сообщений", str(self.stats['messages_committed']))
        table.add_row("Неудачных сообщений", str(self.stats['messages_failed']))
        table.add_row("Успешных коммитов", str(self.stats['commits_successful']))
        table.add_row("Неудачных коммитов", str(self.stats['commits_failed']))
        table.add_row("Ребалансировок", str(self.stats['rebalances']))
        table.add_row("Время работы", f"{duration:.1f} сек")
        
        if duration > 0:
            rate = self.stats['messages_processed'] / duration
            table.add_row("Скорость обработки", f"{rate:.2f} сообщ/сек")
        
        console.print(table)
        
        # Показываем текущие pending offsets
        with self.lock:
            if self.pending_offsets:
                console.print(f"[yellow]📋 [{self.consumer_id}] Ожидающие коммита: {sum(len(v) for v in self.pending_offsets.values())} сообщений[/yellow]")
    
    def close(self):
        """Корректное закрытие"""
        console.print(f"\n[yellow]🔒 [{self.consumer_id}] Завершение работы...[/yellow]")
        
        # Коммитим все оставшиеся offsets
        self.commit_offsets(force=True)
        
        # Выводим итоговую статистику
        console.print(f"\n[bold green]📈 [{self.consumer_id}] ИТОГОВАЯ СТАТИСТИКА:[/bold green]")
        self.show_stats()
        
        # Закрываем консюмер
        self.consumer.close()
        console.print(f"[green]✅ [{self.consumer_id}] Консюмер закрыт[/green]")

if __name__ == "__main__":
    # Конфигурация
    BOOTSTRAP_SERVERS = "localhost:9092"
    GROUP_ID = "manual-commit-group"
    TOPIC = "user-actions"
    
    # Проверяем существование топика
    if not check_topic_exists(TOPIC):
        console.print("[red]❌ Топик 'user-actions' не найден![/red]")
        console.print("[yellow]📌 Запустите сначала подготовку окружения:[/yellow]")
        console.print("[yellow]   python3 setup_demo.py[/yellow]")
        sys.exit(1)
    
    # Получаем ID консюмера из аргументов командной строки
    import argparse
    parser = argparse.ArgumentParser(description='Kafka Consumer с ручным коммитом')
    parser.add_argument('--id', default='consumer-1', help='ID консюмера')
    parser.add_argument('--messages', type=int, default=200, help='Количество сообщений для обработки')
    args = parser.parse_args()
    
    # Создаем и запускаем консюмер
    consumer = ManualCommitConsumer(BOOTSTRAP_SERVERS, GROUP_ID, TOPIC, args.id)
    consumer.consume(max_messages=args.messages)
