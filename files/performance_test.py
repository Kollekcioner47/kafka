#!/usr/bin/env python3
"""
Тестирование производительности продюсеров с разными конфигурациями
Сравнение различных настроек для оптимизации
"""

from confluent_kafka import Producer
import json
import time
import statistics
from datetime import datetime
from rich.console import Console
from rich.table import Table
import threading

console = Console()

class PerformanceTester:
    def __init__(self, bootstrap_servers, topic):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        
    def create_producer(self, config_name, config):
        """Создание продюсера с заданной конфигурацией"""
        base_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'acks': 'all',
        }
        base_config.update(config)
        
        return Producer(base_config), config_name
    
    def run_test(self, producer, config_name, num_messages=10000, message_size=500):
        """
        Запуск теста производительности
        
        Args:
            producer: объект продюсера
            config_name: имя конфигурации
            num_messages: количество сообщений
            message_size: размер полезной нагрузки ('x' * message_size), в байтах;
                          фактический вызов в run_all_tests использует 500 байт
        """
        console.print(f"\n[bold]🧪 Тестирование конфигурации: {config_name}[/bold]")
        
        stats = {
            'sent': 0,
            'confirmed': 0,
            'failed': 0,
            'latencies': [],
            'start_time': None,
            'end_time': None,
        }
        
        # Callback для сбора статистики
        def delivery_callback(err, msg):
            if err:
                stats['failed'] += 1
            else:
                stats['confirmed'] += 1
                # msg.timestamp() возвращает кортеж (timestamp_type, milliseconds).
                # При timestamp_type == 1 (CreateTime, тип времени топика по умолчанию)
                # второй элемент — время в миллисекундах, установленное клиентом
                # в момент вызова produce(). Значит latency = время подтверждения
                # (сейчас, в мс) минус время вызова produce() — миллисекунды.
                ts_type, ts_ms = msg.timestamp()
                if ts_type == 1 and ts_ms:
                    stats['latencies'].append(time.time() * 1000 - ts_ms)
        
        # Генерация тестового сообщения
        test_payload = 'x' * message_size
        
        stats['start_time'] = time.time()
        
        # Отправка сообщений
        for i in range(num_messages):
            message = {
                'id': i + 1,
                'timestamp': datetime.now().isoformat(),
                'payload': test_payload,
                'config': config_name,
                'sequence': i
            }
            
            value = json.dumps(message)
            
            # Время отправки отдельно не храним: момент produce() зафиксирован
            # самим клиентом в timestamp сообщения (см. delivery_callback выше).
            producer.produce(
                topic=self.topic,
                key=str(i % 100),  # 100 разных ключей для распределения
                value=value,
                callback=delivery_callback
            )
            
            stats['sent'] += 1
            
            # Обработка очереди каждые 1000 сообщений
            if i % 1000 == 0:
                producer.poll(0)
        
        # Завершение отправки
        producer.flush()
        stats['end_time'] = time.time()
        
        # Даем время на обработку callback
        time.sleep(1)
        
        # Расчет метрик
        duration = stats['end_time'] - stats['start_time']
        throughput = stats['sent'] / duration if duration > 0 else 0
        
        if stats['latencies']:
            # stats['latencies'] уже в миллисекундах (см. delivery_callback)
            avg_latency = statistics.mean(stats['latencies'])
            p95_latency = statistics.quantiles(stats['latencies'], n=20)[18] if len(stats['latencies']) > 20 else 0
        else:
            avg_latency = p95_latency = 0
        
        return {
            'config': config_name,
            'sent': stats['sent'],
            'confirmed': stats['confirmed'],
            'failed': stats['failed'],
            'duration': duration,
            'throughput': throughput,
            'avg_latency': avg_latency,
            'p95_latency': p95_latency,
            'success_rate': stats['confirmed'] / stats['sent'] if stats['sent'] > 0 else 0
        }
    
    def run_all_tests(self):
        """Запуск всех тестов производительности"""
        console.print("[bold green]🚀 ЗАПУСК ТЕСТОВ ПРОИЗВОДИТЕЛЬНОСТИ[/bold green]")
        
        # Определяем тестовые конфигурации
        test_configs = [
            ('Базовый', {
                'acks': 'all',
                'retries': 5,
                'linger.ms': 5,
                'batch.size': 16384,
            }),
            ('Высокая производительность', {
                'acks': '1',
                'retries': 3,
                'linger.ms': 1,
                'batch.size': 65536,
                'compression.type': 'snappy',
                'queue.buffering.max.messages': 100000,
            }),
            ('Максимальная надежность', {
                'acks': 'all',
                'retries': 10,
                'linger.ms': 20,
                'batch.size': 32768,
                'compression.type': 'lz4',
                'max.in.flight.requests.per.connection': 1,
                'enable.idempotence': True,
            }),
            ('Низкая задержка', {
                'acks': '1',
                'retries': 2,
                'linger.ms': 0,
                'batch.size': 8192,
                'compression.type': 'none',
                'queue.buffering.max.ms': 0,
            }),
            ('Оптимизированный для больших сообщений', {
                'acks': 'all',
                'retries': 5,
                'linger.ms': 10,
                'batch.size': 131072,
                'compression.type': 'gzip',
                'message.max.bytes': 1048576,
            }),
        ]
        
        results = []
        
        # Запускаем тесты для каждой конфигурации
        for config_name, config in test_configs:
            producer, name = self.create_producer(config_name, config)
            result = self.run_test(producer, name, num_messages=2000, message_size=500)
            results.append(result)
            
            # Пауза между тестами
            time.sleep(2)
        
        # Вывод результатов
        self.print_results_table(results)
        
        # Рекомендации
        self.print_recommendations(results)
        
        return results
    
    def print_results_table(self, results):
        """Вывод результатов в виде таблицы"""
        table = Table(title="📊 РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ ПРОИЗВОДИТЕЛЬНОСТИ")
        
        table.add_column("Конфигурация", style="cyan", no_wrap=True)
        table.add_column("Пропускная способность", style="green", justify="right")
        table.add_column("Avg Latency", style="magenta", justify="right")
        table.add_column("P95 Latency", style="yellow", justify="right")
        table.add_column("Успешность", style="blue", justify="right")
        table.add_column("Время", style="white", justify="right")
        
        for result in results:
            table.add_row(
                result['config'],
                f"{result['throughput']:.2f} сообщ/сек",
                f"{result['avg_latency']:.2f} мс",
                f"{result['p95_latency']:.2f} мс",
                f"{result['success_rate']*100:.1f}%",
                f"{result['duration']:.2f} сек"
            )
        
        console.print(table)
    
    def print_recommendations(self, results):
        """Вывод рекомендаций на основе результатов"""
        console.print("\n[bold]🎯 РЕКОМЕНДАЦИИ ПО КОНФИГУРАЦИИ:[/bold]")
        
        # Находим лучшую конфигурацию по каждому параметру
        best_throughput = max(results, key=lambda x: x['throughput'])
        best_latency = min(results, key=lambda x: x['avg_latency'])
        best_reliability = max(results, key=lambda x: x['success_rate'])
        
        console.print(f"\n✅ Для максимальной производительности ({best_throughput['throughput']:.2f} сообщ/сек):")
        console.print(f"   Используйте конфигурацию: [green]{best_throughput['config']}[/green]")
        console.print(f"   Рекомендуется для: логирование, метрики, аналитика")
        
        console.print(f"\n⚡ Для минимальной задержки ({best_latency['avg_latency']:.2f} мс):")
        console.print(f"   Используйте конфигурацию: [cyan]{best_latency['config']}[/cyan]")
        console.print(f"   Рекомендуется для: реальные уведомления, чаты, онлайн-игры")
        
        console.print(f"\n🛡️ Для максимальной надежности ({best_reliability['success_rate']*100:.1f}%):")
        console.print(f"   Используйте конфигурацию: [yellow]{best_reliability['config']}[/yellow]")
        console.print(f"   Рекомендуется для: финансовые транзакции, заказы, critical данные")
        
        console.print(f"\n[bold]📝 ОБЩИЕ РЕКОМЕНДАЦИИ:[/bold]")
        console.print("1. Для высокой нагрузки используйте сжатие (snappy/lz4)")
        console.print("2. Для exactly-once семантики включайте enable.idempotence")
        console.print("3. Настраивайте batch.size под размер сообщений")
        console.print("4. Мониторьте latency и throughput в production")
        console.print("5. Тестируйте разные конфигурации под вашу нагрузку")

if __name__ == "__main__":
    # Конфигурация
    BOOTSTRAP_SERVERS = "kafka1.lab:9092,kafka2.lab:9092,kafka3.lab:9092"
    TOPIC = "test-perf"
    
    # Запускаем тесты
    tester = PerformanceTester(BOOTSTRAP_SERVERS, TOPIC)
    results = tester.run_all_tests()
    
    console.print("\n[bold green]✅ ТЕСТИРОВАНИЕ ЗАВЕРШЕНО![/bold green]")
    
    # Сохраняем результаты в файл
    import csv
    with open('performance_results.csv', 'w', newline='') as csvfile:
        fieldnames = ['config', 'throughput', 'avg_latency', 'p95_latency', 'success_rate', 'duration']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for result in results:
            writer.writerow({
                'config': result['config'],
                'throughput': result['throughput'],
                'avg_latency': result['avg_latency'],
                'p95_latency': result['p95_latency'],
                'success_rate': result['success_rate'],
                'duration': result['duration']
            })
    
    console.print(f"📁 Результаты сохранены в performance_results.csv")
