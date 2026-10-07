#!/usr/bin/env python3
"""
Асинхронный продюсер с правильным измерением Latency.
Используем словарь для хранения времени отправки.
"""

from confluent_kafka import Producer
import json
import time
import statistics
from collections import defaultdict
from rich.console import Console
from rich.progress import Progress

console = Console()

class AsyncProducer:
    def __init__(self, bootstrap_servers):
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'acks': 'all',
            'linger.ms': 1,          # Маленькая задержка для батчинга
            'batch.size': 65536,     # 64KB батчи
            'compression.type': 'snappy',
            # Важно! Буферизируем много сообщений в памяти перед отправкой
            'queue.buffering.max.messages': 100000, 
        }
        self.producer = Producer(self.config)
        
        # Хранилище для замеров времени: Key -> Время отправки
        self.pending_times = defaultdict(float)
        self.latencies = []
        self.confirmed_count = 0

    def delivery_report(self, err, msg):
        """
        Callback вызывается librdkafka в отдельном потоке.
        """
        if err:
            console.print(f"[red]Error: {err}[/red]")
            return

        # Извлекаем ключ сообщения
        try:
            key = msg.key().decode('utf-8')
        except:
            return

        # Вычисляем latency
        send_time = self.pending_times.pop(key, 0)
        if send_time > 0:
            latency = time.time() - send_time
            self.latencies.append(latency)
            self.confirmed_count += 1

    def send_batch(self, topic, num_messages=10000):
        console.print(f"⚡ Асинхронная отправка {num_messages} сообщений...")
        console.print("⚠️  Latency (задержка) считается реальная, от send() до ack().")
        
        start_total = time.time()
        
        with Progress() as progress:
            task = progress.add_task("Sending...", total=num_messages)
            
            for i in range(num_messages):
                key = str(i)
                
                # 1. Запоминаем время отправки ПЕРЕД вызовом produce
                self.pending_times[key] = time.time()
                
                # 2. Асинхронная отправка (моментальная, кладет в очередь librdkafka)
                self.producer.produce(
                    topic, 
                    key=key, 
                    value=json.dumps({'id': i}), 
                    callback=self.delivery_report
                )
                
                # poll() позволяет callback'ам выполниться, чтобы не переполнить память
                if i % 5000 == 0:
                    self.producer.poll(0)
                
                progress.update(task, advance=1)

        console.print("⏳ Ожидание подтверждения всех сообщений (flush)...")
        self.producer.flush() # Ждем пока все улетит и callback'и отработают
        
        duration_total = time.time() - start_total
        
        # Статистика
        if self.latencies:
            avg_lat_ms = statistics.mean(self.latencies) * 1000
            p95_lat_ms = statistics.quantiles(self.latencies, n=20)[18] * 1000 if len(self.latencies) > 20 else 0
        else:
            avg_lat_ms = 0
            p95_lat_ms = 0

        console.print(f"\n[bold green]Результаты:[/bold green]")
        console.print(f"  Throughput: {num_messages/duration_total:.2f} msg/s")
        console.print(f"  Avg Latency: [cyan]{avg_lat_ms:.2f} ms[/cyan]")
        console.print(f"  P95 Latency: [yellow]{p95_lat_ms:.2f} ms[/yellow]")
        console.print(f"  Confirmed: {self.confirmed_count}/{num_messages}")

if __name__ == "__main__":
    BOOTSTRAP_SERVERS = "kafka1.lab:9092,kafka2.lab:9092,kafka3.lab:9092"
    prod = AsyncProducer(BOOTSTRAP_SERVERS)
    prod.send_batch("test-perf")
