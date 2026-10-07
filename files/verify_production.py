#!/usr/bin/env python3
"""
Читает сообщения из топика, чтобы убедиться, что продюсер все отправил.
Завершается по таймауту: читает, пока поступают сообщения, но не дольше timeout_sec.
"""

from confluent_kafka import Consumer, KafkaError
import sys
import time
from rich.console import Console

console = Console()

def consume_messages(bootstrap_servers, topic, timeout_sec=10):
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': f'verify-group-{int(time.time())}', # Уникальный ID группы
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }
    
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    
    msg_count = 0
    timed_out = False
    deadline = time.time() + timeout_sec
    try:
        console.print(f"🔍 Читаем из топика [bold]{topic}[/bold] (таймаут {timeout_sec} сек)...")
        while time.time() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue # Конец партиции
                else:
                    console.print(f"[red]Consumer error: {msg.error()}[/red]")
                    timed_out = False
                    break
            
            msg_count += 1
            if msg_count % 1000 == 0:
                console.print(f"📦 Прочитано: {msg_count}...", end='\r')
    except KeyboardInterrupt:
        timed_out = False
        pass
    finally:
        consumer.close()
    
    if timed_out:
        console.print(f"\n⏰ Таймаут {timeout_sec} сек истёк (сообщений больше нет или они не появились)")
    console.print(f"\n✅ Всего прочитано сообщений: [bold green]{msg_count}[/bold green]")
    return msg_count

if __name__ == "__main__":
    BOOTSTRAP_SERVERS = "kafka1.lab:9092,kafka2.lab:9092,kafka3.lab:9092"
    
    console.print("\n=== ПРОВЕРКА ТОПИКОВ ===")
    consume_messages(BOOTSTRAP_SERVERS, "test-producer", timeout_sec=5)
    # test-perf может содержать много сообщений, ограничим чтение
    console.print("\n(Чтение test-perf ограничено таймаутом, так как там ~10k+ сообщений)")
    consume_messages(BOOTSTRAP_SERVERS, "test-perf", timeout_sec=5)
