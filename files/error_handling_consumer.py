#!/usr/bin/env python3
"""
КОНСЮМЕР С ПРОДВИНУТОЙ ОБРАБОТКОЙ ОШИБОК И РЕЗЕРВНЫМИ СТРАТЕГИЯМИ

Что демонстрирует этот скрипт:
1. Circuit Breaker паттерн для защиты от каскадных сбоев
2. Дедупликацию сообщений (idempotent consumer)
3. Retry логику с экспоненциальной задержкой
4. Dead Letter Queue (DLQ) с отправкой в отдельный топик
5. Валидацию входящих сообщений
6. Мониторинг здоровья и статистику обработки
7. Генерацию тестовых сообщений с преднамеренными ошибками

Концепции Kafka:
- Production-ready обработка ошибок
- Изоляция сбоев (Circuit Breaker)
- Идемпотентность обработки
- Dead Letter Queues для проблемных сообщений
- Валидация схемы сообщений

Паттерны обработки ошибок:
1. Circuit Breaker: временно останавливает обработку при частых ошибках
2. Retry with backoff: повторные попытки с увеличением задержки
3. Dead Letter Queue: сохранение неудачных сообщений для анализа
4. Validation: проверка сообщений перед обработкой
5. Deduplication: предотвращение повторной обработки

Запуск (демонстрация выполняется на ОТДЕЛЬНОМ топике user-actions-error):
python3 error_handling_consumer.py --produce --messages 30

Зачем отдельный топик:
- Демо само создаёт (идемпотентно) топик user-actions-error и DLQ-топик
  user-actions-error-dlq. Оно НЕ читает общий user-actions с 5000
  исторических сообщений из setup_demo.py: в противном случае «битые»
  сообщения дописывались бы в хвост и в первом прогоне не достигались бы
  (группа читала бы старые валидные события, начиная с offset 0).
- --produce генерирует РОВНО args.messages тестовых сообщений («битые» и
  «здоровые» перемешаны) ДО подписки консюмера; --messages задаёт, сколько
  сообщений обработать. По умолчанию оба числа — 30, поэтому «Сгенерировано
  30» и «обработано 30» сходятся.

Результат:
Будут созданы тестовые сообщения (включая "плохие"), обработаны с
применением всех стратегий обработки ошибок, создан DLQ топик и сохранена
статистика в файл consumer_stats.json.

Про гарантии и retry:
- Сообщение обрабатывается в ГЛАВНОМ цикле: poll -> обработка (ограниченные
  inline-retry с экспоненциальной задержкой) -> commit. Offset коммитится
  только ПОСЛЕ терминального исхода сообщения (SUCCESS / DLQ / SKIP),
  поэтому групповая позиция не может «убежать» вперёд ещё не разрешённого
  сообщения, и после рестарта оно не теряется.
- Фоновой retry-очереди и фоновых потоков нет: confluent-kafka Consumer
  не потокобезопасен, коммит/статистика живут в одном потоке.
"""

from confluent_kafka import Consumer, KafkaError, Producer
import json
import time
import signal
import sys
from enum import Enum
from dataclasses import dataclass
from typing import Optional, Dict, Any
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
import hashlib
import subprocess
import argparse

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

def ensure_error_demo_topic(topic="user-actions-error"):
    """Идемпотентное создание отдельного топика демонстрации (раздел 4)
    
    Демо читает и пишет в СВОЙ свежий топик, а не в user-actions с 5000
    исторических сообщений из setup_demo.py — иначе «битые» сообщения
    оказались бы в хвосте и не были бы достигнуты в первом прогоне.
    Повторный запуск безопасен: "already exists" не считается ошибкой.
    """
    try:
        result = subprocess.run(
            ['/opt/kafka/bin/kafka-topics.sh', '--create',
             '--topic', topic,
             '--bootstrap-server', 'localhost:9092',
             '--partitions', '3',
             '--replication-factor', '3'],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0 or "already exists" in result.stderr:
            console.print(f"[green]✅ Топик {topic} готов[/green]")
            return True
        else:
            console.print(f"[yellow]⚠️  Не удалось создать топик {topic}: {result.stderr.strip()}[/yellow]")
            return False
    except Exception as e:
        console.print(f"[yellow]⚠️  Ошибка создания топика: {e}[/yellow]")
        return False

class ProcessingResult(Enum):
    """Результат обработки сообщения"""
    SUCCESS = "success"
    RETRY = "retry"
    DLQ = "dlq"  # Dead Letter Queue
    SKIP = "skip"
    FATAL = "fatal"

@dataclass
class ProcessedMessage:
    """Информация об обработанном сообщении"""
    partition: int
    offset: int
    result: ProcessingResult
    processing_time: float
    error: Optional[str] = None
    retry_count: int = 0

class TestMessageProducer:
    """Продюсер тестовых сообщений"""
    def __init__(self, topic="user-actions", bootstrap_servers="localhost:9092"):
        self.producer = Producer({'bootstrap.servers': bootstrap_servers})
        self.topic = topic
        self.message_count = 0
    
    def produce_messages(self, num_messages=100):
        """Создание тестовых сообщений"""
        console.print(f"[cyan]📨 Генерирую {num_messages} тестовых сообщений...[/cyan]")
        
        for i in range(num_messages):
            message = {
                'event_id': f'error-test-{int(time.time())}-{i}',
                'user_id': f'user_{i % 50}',
                'action': ['login', 'logout', 'purchase', 'view', 'search', 'click', 'add_to_cart'][i % 7],
                'timestamp': time.time() * 1000,
                'data': {
                    'page': f'/page/{i % 10}',
                    'amount': round(i * 1.5, 2) if i % 3 == 0 else None,
                    'test_type': 'error_handling_demo'
                }
            }
            
            # Намеренно создаем 10% "плохих" сообщений для демонстрации обработки ошибок
            if i % 10 == 0:
                message['action'] = 'invalid_action'  # Невалидное действие
            elif i % 13 == 0:
                message.pop('user_id')  # Отсутствует обязательное поле
            elif i % 17 == 0:
                message['timestamp'] = 'invalid_timestamp'  # Невалидный timestamp
            
            self.producer.produce(
                self.topic,
                key=str(i % 6),
                value=json.dumps(message, ensure_ascii=False),
                callback=self.delivery_callback
            )
            self.producer.poll(0)
        
        self.producer.flush()
        console.print(f"[green]✅ Сгенерировано {self.message_count} тестовых сообщений[/green]")
    
    def delivery_callback(self, err, msg):
        if err:
            console.print(f"[red]❌ Ошибка доставки: {err}[/red]")
        else:
            self.message_count += 1
            if self.message_count % 20 == 0:
                console.print(f"[cyan]📨 Отправлено {self.message_count} сообщений...[/cyan]")

class ErrorHandlingConsumer:
    def __init__(self, bootstrap_servers, group_id, topic, produce_test_messages=0):
        """
        Консюмер с продвинутой обработкой ошибок
        
        Args:
            bootstrap_servers: адреса брокеров
            group_id: ID группы
            topic: топик для подписки
            produce_test_messages: сколько тестовых сообщений сгенерировать
                в consume() перед подпиской (0 — не генерировать)
        """
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',  # Читаем с начала
            'enable.auto.commit': False,
            'session.timeout.ms': 10000,
            'max.poll.interval.ms': 300000,
            'isolation.level': 'read_committed',
        }
        
        self.consumer = Consumer(self.config)
        self.topic = topic
        self.running = True
        
        # Producer для отправки в DLQ
        self.dlq_producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'acks': 'all',
        })
        
        # Статистика и мониторинг
        self.stats = {
            'total_messages': 0,
            'success': 0,
            'retries': 0,
            'dlq': 0,
            'skipped': 0,
            'fatal': 0,
            'processing_time': 0,
            'errors_by_type': {},
            'start_time': time.time(),
        }
        
        # Сколько тестовых сообщений сгенерировать (0 — не генерировать).
        # Сама генерация выполняется в consume() ПОСЛЕ создания топика и ДО
        # подписки: консюмер (auto.offset.reset=earliest) прочитает именно
        # свежую порцию, а не историю общего топика.
        self.produce_test_messages = produce_test_messages
        
        # Circuit Breaker состояние (мутируется только в главном потоке)
        self.circuit_state = {
            'open': False,
            'failure_count': 0,
            'last_failure': 0,
            'half_open_time': 0,
        }
        
        # Хэш-таблица для дедупликации
        self.processed_messages = set()
        
        # Обработка сигналов
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        # NB: фоновых потоков (retry-очередь, мониторинг) здесь НЕТ.
        # confluent-kafka Consumer не потокобезопасен, поэтому вся работа
        # (poll -> inline-retry -> commit) выполняется в одном главном
        # потоке consume() — см. методы process_message_with_retry()/consume().
    
    def signal_handler(self, signum, frame):
        """Обработчик сигналов"""
        console.print(f"\n[yellow]⚠️  Получен сигнал {signum}, graceful shutdown...[/yellow]")
        self.running = False
    
    def message_hash(self, msg):
        """Создание хэша сообщения для дедупликации"""
        content = f"{msg.partition()}:{msg.offset()}:{msg.value()}"
        return hashlib.md5(content.encode()).hexdigest()
    
    def check_circuit_breaker(self):
        """Проверка состояния Circuit Breaker"""
        if self.circuit_state['open']:
            # Если Circuit Breaker открыт, проверяем можно ли перейти в half-open
            if time.time() - self.circuit_state['last_failure'] > 30:  # 30 секунд
                self.circuit_state['open'] = False
                self.circuit_state['half_open_time'] = time.time()
                # Сбрасываем счётчик ошибок, чтобы после half-open не открыться
                # сразу снова (иначе «прощупывание» было бы бесполезным).
                self.circuit_state['failure_count'] = 0
                console.print("[yellow]🔄 Circuit Breaker переходит в half-open состояние[/yellow]")
                return True
            return False
        
        # Если слишком много ошибок, открываем Circuit Breaker
        if self.circuit_state['failure_count'] >= 5:
            self.circuit_state['open'] = True
            self.circuit_state['last_failure'] = time.time()
            console.print("[red]🔴 Circuit Breaker открыт! Пауза 30 секунд[/red]")
            return False
        
        return True
    
    def record_failure(self, error_type):
        """Запись ошибки для Circuit Breaker"""
        self.circuit_state['failure_count'] += 1
        
        if error_type not in self.stats['errors_by_type']:
            self.stats['errors_by_type'][error_type] = 0
        self.stats['errors_by_type'][error_type] += 1
    
    def record_success(self):
        """Запись успеха для Circuit Breaker"""
        if self.circuit_state['failure_count'] > 0:
            self.circuit_state['failure_count'] -= 1
    
    def validate_message(self, data):
        """Валидация сообщения"""
        required_fields = ['event_id', 'user_id', 'action', 'timestamp']
        
        for field in required_fields:
            if field not in data:
                return False, f"Missing required field: {field}"
        
        # Проверка формата timestamp
        try:
            # Пробуем преобразовать timestamp
            ts = float(data['timestamp'])
            if ts <= 0:
                return False, "Invalid timestamp value"
        except (ValueError, TypeError):
            return False, "Invalid timestamp format"
        
        # Проверка action
        valid_actions = ['login', 'logout', 'purchase', 'view', 'search', 'click', 'add_to_cart']
        if data['action'] not in valid_actions:
            return False, f"Invalid action: {data['action']}"
        
        return True, "OK"
    
    def business_logic(self, data):
        """Бизнес-логика обработки сообщения"""
        # Имитация различных сценариев обработки
        
        # 1. Покупки обрабатываем долго
        if data['action'] == 'purchase':
            time.sleep(0.1)  # 100ms
            if 'data' not in data or 'amount' not in data['data']:
                raise ValueError("Purchase missing amount")
        
        # 2. Логины проверяем на частоту
        elif data['action'] == 'login':
            time.sleep(0.05)  # 50ms
        
        # 3. ~10% сообщений генерируют ВРЕМЕННУЮ ошибку (retry-кейс): слово
        #    "temporary"/"timeout" в тексте направляет её в retry-ветку, а hash(event_id)
        #    стабилен в рамках процесса — сообщение падает на каждой из 3 попыток и после
        #    их исчерпания уходит в DLQ. Так наглядно видны backoff (1с, 2с) и итоговый DLQ.
        elif hash(data.get('event_id', '')) % 100 < 10:
            raise Exception("Temporary processing failure (timeout)")
        
        # 4. Обычные действия
        else:
            time.sleep(0.02)  # 20ms
        
        return True
    
    def process_message(self, msg) -> ProcessedMessage:
        """
        Обработка одного сообщения с полным циклом error handling
        
        Args:
            msg: сообщение Kafka
        
        Returns:
            ProcessedMessage: результат обработки
        """
        start_time = time.time()
        
        if msg.value() is None:
            console.print(f"[yellow]⚠️  Пустое сообщение, пропускаем[/yellow]")
            return ProcessedMessage(
                partition=msg.partition(),
                offset=msg.offset(),
                result=ProcessingResult.SKIP,
                processing_time=time.time() - start_time
            )
        
        msg_hash = self.message_hash(msg)
        
        # Шаг 1: Проверка дедупликации
        if msg_hash in self.processed_messages:
            console.print(f"[yellow]⚠️  Дубликат сообщения, пропускаем[/yellow]")
            return ProcessedMessage(
                partition=msg.partition(),
                offset=msg.offset(),
                result=ProcessingResult.SKIP,
                processing_time=time.time() - start_time
            )
        
        try:
            # Шаг 2: Декодирование
            value = msg.value().decode('utf-8')
            data = json.loads(value)
            
            # Шаг 3: Валидация
            is_valid, validation_error = self.validate_message(data)
            if not is_valid:
                console.print(f"[red]❌ Ошибка валидации: {validation_error}[/red]")
                console.print(f"[dim]   Данные: {json.dumps(data, ensure_ascii=False)[:100]}...[/dim]")
                self.record_failure("validation")
                return ProcessedMessage(
                    partition=msg.partition(),
                    offset=msg.offset(),
                    result=ProcessingResult.DLQ,
                    processing_time=time.time() - start_time,
                    error=validation_error
                )
            
            # Шаг 4: Бизнес-логика
            self.business_logic(data)
            
            # Шаг 5: Успешная обработка
            self.processed_messages.add(msg_hash)
            self.record_success()
            
            console.print(f"[green]✅ Успешно обработано: {data.get('action', 'N/A')} (user: {data.get('user_id', 'N/A')[:8]}...)[/green]")
            
            return ProcessedMessage(
                partition=msg.partition(),
                offset=msg.offset(),
                result=ProcessingResult.SUCCESS,
                processing_time=time.time() - start_time
            )
            
        except json.JSONDecodeError as e:
            error_msg = f"JSON decode error: {e}"
            console.print(f"[red]❌ {error_msg}[/red]")
            self.record_failure("json_decode")
            return ProcessedMessage(
                partition=msg.partition(),
                offset=msg.offset(),
                result=ProcessingResult.DLQ,
                processing_time=time.time() - start_time,
                error=error_msg
            )
            
        except KeyError as e:
            error_msg = f"Missing key: {e}"
            console.print(f"[red]❌ {error_msg}[/red]")
            self.record_failure("missing_key")
            return ProcessedMessage(
                partition=msg.partition(),
                offset=msg.offset(),
                result=ProcessingResult.RETRY,
                processing_time=time.time() - start_time,
                error=error_msg,
                retry_count=1
            )
            
        except ValueError as e:
            error_msg = f"Value error: {e}"
            console.print(f"[red]❌ {error_msg}[/red]")
            self.record_failure("value_error")
            return ProcessedMessage(
                partition=msg.partition(),
                offset=msg.offset(),
                result=ProcessingResult.DLQ,
                processing_time=time.time() - start_time,
                error=error_msg
            )
            
        except Exception as e:
            error_msg = f"Processing error: {e}"
            console.print(f"[red]❌ {error_msg}[/red]")
            self.record_failure("processing")
            
            # Для некоторых ошибок пробуем retry
            if "temporary" in str(e).lower() or "timeout" in str(e).lower():
                return ProcessedMessage(
                    partition=msg.partition(),
                    offset=msg.offset(),
                    result=ProcessingResult.RETRY,
                    processing_time=time.time() - start_time,
                    error=error_msg,
                    retry_count=1
                )
            else:
                return ProcessedMessage(
                    partition=msg.partition(),
                    offset=msg.offset(),
                    result=ProcessingResult.DLQ,
                    processing_time=time.time() - start_time,
                    error=error_msg
                )
    
    def send_to_dlq(self, msg, error):
        """Отправка сообщения в Dead Letter Queue"""
        dlq_message = {
            'original_message': msg.value().decode('utf-8') if msg.value() else None,
            'error': error,
            'original_topic': self.topic,
            'original_partition': msg.partition(),
            'original_offset': msg.offset(),
            'timestamp': time.time(),
            'dlq_topic': f"{self.topic}-dlq",
        }
        
        try:
            self.dlq_producer.produce(
                topic=f"{self.topic}-dlq",
                key=msg.key(),
                value=json.dumps(dlq_message),
                callback=lambda err, m: None
            )
            self.dlq_producer.poll(0)
            return True
        except Exception as e:
            console.print(f"[red]❌ Ошибка отправки в DLQ: {e}[/red]")
            return False
    
    def process_message_with_retry(self, msg, max_attempts=3):
        """
        Обработка одного сообщения с ОГРАНИЧЕННЫМИ СИНХРОННЫМИ retry
        (inline) в главном цикле консюмера.
        
        ВАЖНО про offset'ы: сообщение НЕ покидает главный поток — следующий
        poll() начнётся только после терминального исхода текущего сообщения,
        а offset коммитится в consume() ПОСЛЕ возврата из этого метода. Поэтому
        групповая позиция не может «убежать» вперёд неразрешённого offset N
        (как было с фоновой retry-очередью: основной цикл коммитил N+1, N+2,
        ..., и N терялся при рестарте навсегда).
        
        Args:
            msg: сообщение Kafka
            max_attempts: максимум попыток (включая первую)
        
        Returns:
            ProcessedMessage: ВСЕГДА терминальный исход (SUCCESS/DLQ/SKIP).
            RETRY после исчерпания попыток превращается в DLQ.
        """
        attempt = 0
        while True:
            attempt += 1
            processed_msg = self.process_message(msg)
            
            if processed_msg.result != ProcessingResult.RETRY:
                # Терминальный исход: SUCCESS / DLQ / SKIP / FATAL
                return processed_msg
            
            if attempt >= max_attempts:
                # Попытки исчерпаны — сообщение уходит в DLQ (его offset будет
                # закоммичен в consume() после отправки в DLQ)
                error_msg = f"Max retries exceeded ({max_attempts}): {processed_msg.error}"
                console.print(f"[red]❌ {error_msg} — отправляю в DLQ[/red]")
                self.record_failure("max_retries")
                return ProcessedMessage(
                    partition=msg.partition(),
                    offset=msg.offset(),
                    result=ProcessingResult.DLQ,
                    processing_time=processed_msg.processing_time,
                    error=error_msg,
                    retry_count=attempt,
                )
            
            # Синхронная пауза перед следующей попыткой (экспоненциальный
            # backoff: 1с, 2с, ...). Offset в это время НЕ коммитится.
            delay = 2 ** (attempt - 1)
            console.print(f"[yellow]🔄 Повторная попытка {attempt}/{max_attempts} для offset {msg.offset()} через {delay} с...[/yellow]")
            self.stats['retries'] += 1
            time.sleep(delay)
    
    def update_stats(self, processed_msg):
        """Обновление статистики"""
        self.stats['total_messages'] += 1
        self.stats['processing_time'] += processed_msg.processing_time
        
        if processed_msg.result == ProcessingResult.SUCCESS:
            self.stats['success'] += 1
        elif processed_msg.result == ProcessingResult.RETRY:
            self.stats['retries'] += 1
        elif processed_msg.result == ProcessingResult.DLQ:
            self.stats['dlq'] += 1
        elif processed_msg.result == ProcessingResult.SKIP:
            self.stats['skipped'] += 1
        elif processed_msg.result == ProcessingResult.FATAL:
            self.stats['fatal'] += 1
    
    def show_stats(self):
        """Отображение статистики"""
        duration = time.time() - self.stats['start_time']
        
        table = Table(title="📊 СТАТИСТИКА ОБРАБОТКИ ОШИБОК", width=80)
        table.add_column("Метрика", style="cyan", width=30)
        table.add_column("Значение", style="green", width=50)
        
        table.add_row("Всего сообщений", str(self.stats['total_messages']))
        table.add_row("Успешно", str(self.stats['success']))
        table.add_row("Retry", str(self.stats['retries']))
        table.add_row("DLQ", str(self.stats['dlq']))
        table.add_row("Пропущено", str(self.stats['skipped']))
        table.add_row("Фатальных", str(self.stats['fatal']))
        
        if self.stats['total_messages'] > 0:
            success_rate = (self.stats['success'] / self.stats['total_messages']) * 100
            avg_time = self.stats['processing_time'] / self.stats['total_messages'] * 1000
            
            table.add_row("Успешность", f"{success_rate:.1f}%")
            table.add_row("Среднее время", f"{avg_time:.2f} мс")
        
        table.add_row("Время работы", f"{duration:.1f} сек")
        
        # Circuit Breaker состояние
        cb_status = "🔴 OPEN" if self.circuit_state['open'] else "🟢 CLOSED"
        table.add_row("Circuit Breaker", cb_status)
        
        # Ошибки по типам
        if self.stats['errors_by_type']:
            table.add_row("Ошибки по типам", ", ".join([f"{k}:{v}" for k, v in self.stats['errors_by_type'].items()]))
        
        console.print(table)
    
    def consume(self, max_messages=None):
        """
        Основной цикл потребления.
        
        Всё выполняется в ОДНОМ потоке: poll -> обработка (с ограниченными
        inline-retry) -> commit. Offset коммитится только ПОСЛЕ терминального
        исхода сообщения — групповая позиция не «убегает» вперёд ещё не
        разрешённого сообщения (см. process_message_with_retry).
        """
        console.print(Panel.fit(
            "[bold green]🚀 ЗАПУСК КОНСЮМЕРА С ПРОДВИНУТОЙ ОБРАБОТКОЙ ОШИБОК[/bold green]\n"
            f"👥 Group ID: {self.config['group.id']}\n"
            f"📭 Топик: {self.topic}\n"
            f"🛡️  Стратегии: Circuit Breaker, Retry, DLQ, Дедупликация",
            border_style="green"
        ))
        
        # Создаём DLQ топик (идемпотентно)
        self.create_dlq_topic()
        
        # Если запрошена генерация тестовых сообщений — создаём их ДО
        # подписки. Основной топик уже создан в __main__
        # (ensure_error_demo_topic), поэтому продюсер пишет в существующий
        # топик, а консюмер с auto.offset.reset=earliest гарантированно
        # прочитает именно эту порцию, а не историю общего топика user-actions.
        if self.produce_test_messages:
            producer = TestMessageProducer(self.topic, self.config['bootstrap.servers'])
            producer.produce_messages(self.produce_test_messages)
        
        self.consumer.subscribe([self.topic])
        
        message_count = 0
        
        # Таймаут без данных: выход, если 60 секунд не пришло ни одного
        # сообщения (топик пуст или offset'ы группы уже закоммичены с
        # прошлого запуска). Иначе цикл крутился бы вечно.
        NO_DATA_TIMEOUT = 60
        no_data_since = time.time()
        
        try:
            while self.running and (max_messages is None or message_count < max_messages):
                # Проверяем Circuit Breaker
                if not self.check_circuit_breaker():
                    time.sleep(1)
                    continue
                
                # Получаем сообщение
                msg = self.consumer.poll(timeout=2.0)
                
                if msg is None:
                    if time.time() - no_data_since > NO_DATA_TIMEOUT:
                        console.print(f"\n[yellow]⚠️  Нет сообщений дольше {NO_DATA_TIMEOUT} секунд — прекращаю работу[/yellow]")
                        console.print("[yellow]📌 Проверьте, что продюсер работает, или сбросьте offset'ы группы:[/yellow]")
                        console.print("[yellow]   /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \\[/yellow]")
                        console.print(f"[yellow]     --group {self.config['group.id']} --reset-offsets --to-earliest --execute --topic {self.topic}[/yellow]")
                        console.print("[yellow]   либо сгенерируйте новую порцию данных:[/yellow]")
                        console.print("[yellow]   python3 error_handling_consumer.py --produce --messages 30[/yellow]")
                        break
                    continue
                
                # Сообщение (или событие ошибки) получено — сбрасываем таймер
                no_data_since = time.time()
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        console.print(f"[red]❌ Ошибка Kafka: {msg.error()}[/red]")
                        self.record_failure("kafka")
                        continue
                
                # Обрабатываем сообщение: метод возвращает ТОЛЬКО терминальный
                # исход (RETRY разрешается синхронно ограниченными inline-
                # повторами внутри process_message_with_retry)
                processed_msg = self.process_message_with_retry(msg)
                self.update_stats(processed_msg)
                
                # Если исход — DLQ, отправляем сообщение в DLQ-топик
                if processed_msg.result == ProcessingResult.DLQ:
                    self.send_to_dlq(msg, processed_msg.error)
                
                # Коммитим offset ТОЛЬКО после терминального исхода сообщения.
                # Если бы мы коммитили следующее сообщение, не разрешив текущее
                # (RETRY), групповая позиция ушла бы за offset текущего, и после
                # рестарта сообщение потерялось бы навсегда.
                if processed_msg.result in (ProcessingResult.SUCCESS,
                                            ProcessingResult.DLQ,
                                            ProcessingResult.SKIP):
                    self.consumer.commit(message=msg, asynchronous=False)
                    message_count += 1
                else:
                    # RETRY/FATAL не должны выходить из process_message_with_retry
                    console.print(f"[red]❌ Неожиданный исход обработки: {processed_msg.result}[/red]")
                
                # Показываем статистику каждые 10 сообщений
                if message_count % 10 == 0 and message_count > 0:
                    self.show_stats()
                    console.print(f"[cyan]📈 Обработано: {message_count} сообщений[/cyan]")
        
        except KeyboardInterrupt:
            console.print("\n[yellow]⚠️  Прервано пользователем[/yellow]")
        except Exception as e:
            console.print(f"[red]❌ Критическая ошибка: {e}[/red]")
        finally:
            self.close()
    
    def create_dlq_topic(self):
        """Создание DLQ топика если не существует"""
        try:
            result = subprocess.run(
                ['/opt/kafka/bin/kafka-topics.sh', '--create',
                 '--topic', f'{self.topic}-dlq',
                 '--bootstrap-server', 'localhost:9092',
                 '--partitions', '3',
                 '--replication-factor', '3',
                 '--config', 'retention.ms=604800000'],
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0 or "already exists" in result.stderr:
                console.print(f"[green]✅ DLQ топик {self.topic}-dlq готов[/green]")
            else:
                console.print(f"[yellow]⚠️  Не удалось создать DLQ топик: {result.stderr}[/yellow]")
        except Exception as e:
            console.print(f"[yellow]⚠️  Ошибка создания DLQ топика: {e}[/yellow]")
    
    def close(self):
        """Корректное закрытие консюмера"""
        console.print("\n[yellow]🔒 Завершение работы консюмера...[/yellow]")
        
        # Выводим итоговую статистику
        console.print("\n" + "="*60)
        console.print("[bold green]📈 ИТОГОВАЯ СТАТИСТИКА ОБРАБОТКИ ОШИБОК[/bold green]")
        console.print("="*60)
        self.show_stats()
        
        # Закрываем консюмеры и продюсеры
        self.consumer.close()
        self.dlq_producer.flush()
        
        console.print("\n[green]✅ Консюмер корректно завершен[/green]")
        
        # Сохраняем статистику в файл
        with open('consumer_stats.json', 'w') as f:
            json.dump(self.stats, f, indent=2)
        console.print(f"[cyan]📁 Статистика сохранена в consumer_stats.json[/cyan]")

if __name__ == "__main__":
    # Конфигурация: демо работает на ОТДЕЛЬНОМ топике user-actions-error,
    # а не на общем user-actions (5000 исторических сообщений из setup_demo
    # заслонили бы «битые» сообщения демо — см. докстринг скрипта)
    BOOTSTRAP_SERVERS = "localhost:9092"
    GROUP_ID = "error-handling-group"
    TOPIC = "user-actions-error"
    
    # Парсим аргументы командной строки
    parser = argparse.ArgumentParser(description='Консюмер с обработкой ошибок')
    parser.add_argument('--messages', type=int, default=30, help='Сколько сообщений обработать и (с --produce) сгенерировать')
    parser.add_argument('--produce', action='store_true', help='Сгенерировать тестовые сообщения перед запуском')
    args = parser.parse_args()
    
    # Создаём (идемпотентно) отдельный топик демонстрации: он не зависит от
    # setup_demo.py и создаётся скриптом самостоятельно
    if not ensure_error_demo_topic(TOPIC):
        console.print("[red]❌ Не удалось подготовить топик демонстрации[/red]")
        sys.exit(1)
    
    # Создаем и запускаем консюмер.
    # Число сгенерированных сообщений = args.messages (при --produce), чтобы
    # «Сгенерировано N» и «обработано N» сходились.
    consumer = ErrorHandlingConsumer(
        BOOTSTRAP_SERVERS, 
        GROUP_ID, 
        TOPIC,
        produce_test_messages=args.messages if args.produce else 0
    )
    
    # Обрабатываем указанное количество сообщений
    console.print(f"[cyan]🎯 Цель: обработать {args.messages} сообщений[/cyan]")
    consumer.consume(max_messages=args.messages)
