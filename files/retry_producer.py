#!/usr/bin/env python3
"""
Продюсер с обработкой ошибок и Dead Letter Queue (DLQ).
Клиент confluent-kafka сам обрабатывает ретраи (согласно настройке retries).
Если библиотечные ретраи исчерпаны (фатальная ошибка) — сообщение уходит
в DLQ-топик test-dlq отдельным продюсером, а копия ошибки остаётся
в self.failed_messages для отчёта. На здоровом кластере эта ветка не выполняется.
"""

from confluent_kafka import Producer
import json
import time
from datetime import datetime
from rich.console import Console

console = Console()

class ReliableProducer:
    def __init__(self, bootstrap_servers):
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            # Важно: enable.idempotence гарантирует отсутствие дубликатов при ретраях
            'enable.idempotence': True, 
            'acks': 'all',
            'retries': 5,          # Библиотека сама попробует 5 раз
            'retry.backoff.ms': 100,
            'linger.ms': 5,
        }
        self.producer = Producer(self.config)
        # Отдельный продюсер для DLQ-топика test-dlq (изолируем «плохие» сообщения)
        self.dlq_producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'acks': 'all',
        })
        self.failed_messages = []  # Копия ошибок в памяти для отчёта (в проде — чтение из DLQ-топика)

    def delivery_report(self, err, msg):
        """
        Callback вызывается только когда брокер финально подтвердил успех
        или когда все попытки ретрая (retries) исчерпаны.
        """
        if err is not None:
            console.print(f"[red]❌ Сообщение не доставлено (ретраи исчерпаны): {err}[/red]")
            # Сообщение после N попыток (retries) уходит в DLQ-топик test-dlq.
            # Здесь же сохраняем копию ошибки в память — для отчёта в конце.
            self.failed_messages.append({
                'key': msg.key().decode('utf-8') if msg.key() else None,
                'value': msg.value().decode('utf-8') if msg.value() else None,
                'error': str(err)
            })
            try:
                dlq_value = json.dumps({
                    'key': msg.key().decode('utf-8') if msg.key() else None,
                    'value': msg.value().decode('utf-8') if msg.value() else None,
                    'error': str(err),
                }, ensure_ascii=False)
                # Отправляем в test-dlq отдельным продюсером; подтверждение ждём
                # вызовом flush() ниже, в run().
                self.dlq_producer.produce('test-dlq', key=msg.key(), value=dlq_value)
            except Exception as dlq_err:
                console.print(f"[yellow]⚠️ Не удалось отправить в test-dlq: {dlq_err}[/yellow]")
        else:
            pass # Успех, все хорошо

    def run(self, topic, num_messages=2000):
        console.print(f"🛡️ Запуск надежного продюсера ({num_messages} msg)...")
        console.print("ℹ️  Ретраи обрабатывает библиотека автоматически (retries=5).")
        
        for i in range(num_messages):
            val = json.dumps({'id': i, 'ts': datetime.now().isoformat()})
            self.producer.produce(topic, key=str(i), value=val, callback=self.delivery_report)
            self.producer.poll(0) # Асинхронная отправка
            
        self.producer.flush()
        # Дожидаемся подтверждений отправки DLQ-сообщений в test-dlq (если ошибки были)
        self.dlq_producer.flush()
        
        console.print(f"✅ Завершено. Ошибок (отправлено в test-dlq): {len(self.failed_messages)}")
        if self.failed_messages:
            console.print(self.failed_messages[:5]) # Покажем первые ошибки

if __name__ == "__main__":
    BOOTSTRAP_SERVERS = "kafka1.lab:9092,kafka2.lab:9092,kafka3.lab:9092"
    prod = ReliableProducer(BOOTSTRAP_SERVERS)
    prod.run("test-producer")
