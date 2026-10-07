#!/usr/bin/env python3
"""
Базовый продюсер: Сравнение уровней надежности (acks)
Убрано искусственное замедление для реального теста производительности.
"""

from confluent_kafka import Producer
import json
import time
from datetime import datetime
from faker import Faker
from rich.console import Console
from rich.table import Table

console = Console()
fake = Faker('ru_RU')

class BasicProducer:
    def __init__(self, bootstrap_servers, acks='all'):
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'acks': acks,
            'retries': 5,
            'linger.ms': 5,       # Накапливаем 5мс перед отправкой батча
            'batch.size': 32768,  # 32KB батч
            'compression.type': 'snappy',
        }
        self.producer = Producer(self.config)
        self.stats = {'delivered': 0, 'failed': 0}

    def delivery_report(self, err, msg):
        """Callback обработки доставки"""
        if err is not None:
            self.stats['failed'] += 1
        else:
            self.stats['delivered'] += 1

    def send_data(self, topic, num_messages=5000):
        start_time = time.time()
        console.print(f"🚀 Отправка [bold cyan]{num_messages}[/bold cyan] сообщений с acks=[bold yellow]{self.config['acks']}[/bold yellow]...")
        
        for i in range(num_messages):
            # Генерация данных
            value = json.dumps({
                'id': i,
                'user': fake.name(),
                'ts': datetime.now().isoformat()
            }, ensure_ascii=False)
            
            # Используем ID как ключ для партиционирования
            key = str(i)
            
            self.producer.produce(
                topic=topic,
                key=key,
                value=value,
                callback=self.delivery_report
            )
            
            # poll(0) просто обслуживает callback-очередь, не блокирует отправку
            if i % 1000 == 0:
                self.producer.poll(0)

        # Блокируем завершение, пока все сообщения не уйдут
        self.producer.flush()
        
        duration = time.time() - start_time
        throughput = num_messages / duration
        
        return {
            'acks': self.config['acks'],
            'throughput': throughput,
            'duration': duration,
            'delivered': self.stats['delivered'],
            'failed': self.stats['failed']
        }

if __name__ == "__main__":
    BOOTSTRAP_SERVERS = "kafka1.lab:9092,kafka2.lab:9092,kafka3.lab:9092"
    TOPIC = "test-producer"
    
    results = []
    for acks in ['0', '1', 'all']:
        prod = BasicProducer(BOOTSTRAP_SERVERS, acks=acks)
        # При acks=0 брокер не присылает подтверждение (ack). Как поведёт себя
        # delivery-отчёт на вашей версии confluent-kafka — проверьте фактически:
        # обычно при acks=0 отчёт всё равно приходит (сразу после записи в сокет).
        res = prod.send_data(TOPIC, num_messages=5000)
        results.append(res)
        time.sleep(1)

    # Вывод таблицы
    table = Table(title="Сравнение производительности (Acks)")
    table.add_column("Acks", style="cyan")
    table.add_column("Throughput (msg/s)", justify="right")
    table.add_column("Time (s)", justify="right")
    table.add_column("Status", style="green")

    for r in results:
        # При acks=0 брокер ничего не подтверждает: delivered отражает «отправлено
        # в сокет», а не «подтверждено брокером». Проверьте на вашей версии:
        # delivery-отчёт при acks=0 обычно всё равно приходит — сверьте фактическое
        # значение delivered/failed (код считает его в stats, но в таблицу не выводит).
        status = "Fast & Unsafe" if r['acks'] == '0' else ("Balanced" if r['acks'] == '1' else "Safe & Slow")
        table.add_row(r['acks'], f"{r['throughput']:.2f}", f"{r['duration']:.2f}", status)
    
    console.print(table)
