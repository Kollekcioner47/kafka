#!/usr/bin/env python3
"""
ТЕСТИРОВАНИЕ РЕБАЛАНСИРОВКИ CONSUMER GROUP

Что демонстрирует этот скрипт:
1. Интерактивное тестирование различных сценариев ребалансировки
2. 5 готовых сценариев для изучения поведения consumer groups
3. Мониторинг состояния групп через Kafka CLI
4. Имитацию сбоев консюмеров (kill -9)
5. Изменение количества партиций топика

Сценарии ребалансировки:
1. Добавление нового консюмера в группу
2. Graceful shutdown консюмера
3. Сбой консюмера (имитация аппаратного сбоя)
4. Увеличение количества партиций топика
5. Изменение subscription консюмеров

Концепции Kafka:
- Protocol types (consumer, connect)
- Rebalance protocols (eager, cooperative)
- Partition assignment strategies (range, roundrobin, sticky)
- Session timeouts и heartbeat механизм
- Generation ID и member ID

Запуск:
python3 rebalance_test.py

Результат:
Интерактивная консоль для экспериментов с ребалансировкой.
Можно запускать/останавливать консюмеры и наблюдать за распределением партиций.
"""

import subprocess
import threading
import time
import signal
import sys
import os
from rich.console import Console
from rich.table import Table
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
import json

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

class RebalanceTester:
    def __init__(self):
        self.processes = {}
        self.rebalance_scenarios = [
            "Добавление консюмера",
            "Удаление консюмера", 
            "Сбой консюмера",
            "Увеличение партиций",
            "Изменение subscription"
        ]
        # Топики, созданные сценариями 4-5: удаляются в конце run_scenario,
        # чтобы повторные запуски лабы не оставляли мусор в кластере
        self.scenario_topics_to_clean = []
    
    def start_consumer(self, consumer_id):
        """Запуск консюмера"""
        cmd = [
            sys.executable, 'manual_commit_consumer.py',
            '--id', f'test-{consumer_id}',
            '--messages', '1000'  # Обработает много сообщений
        ]
        
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        
        self.processes[consumer_id] = process
        console.print(f"[green]✅ Запущен консюмер test-{consumer_id} (PID: {process.pid})[/green]")
        return process
    
    def stop_consumer(self, consumer_id):
        """Остановка консюмера"""
        if consumer_id in self.processes:
            process = self.processes[consumer_id]
            process.terminate()
            process.wait()
            console.print(f"[yellow]🛑 Остановлен консюмер test-{consumer_id}[/yellow]")
            del self.processes[consumer_id]
    
    def kill_consumer(self, consumer_id):
        """Сильное завершение консюмера (имитация сбоя)"""
        if consumer_id in self.processes:
            process = self.processes[consumer_id]
            process.kill()  # SIGKILL вместо SIGTERM
            console.print(f"[red]💥 Убит консюмер test-{consumer_id} (имитация сбоя)[/red]")
            del self.processes[consumer_id]
    
    def check_consumer_group(self):
        """Проверка состояния consumer group через CLI
        
        Используем обычный --describe: колонки CONSUMER-ID/HOST наглядно
        показывают, как партиции перераспределяются между консюмерами.
        НЕ используем '--describe --state' без значения: в Kafka 4.x опция
        --state — это ФИЛЬТР, требующий значения (Stable, Empty и т.п.),
        а не переключатель — без значения команда завершится ошибкой.
        """
        try:
            result = subprocess.run(
                ['/opt/kafka/bin/kafka-consumer-groups.sh',
                 '--bootstrap-server', 'localhost:9092',
                 '--group', 'manual-commit-group',
                 '--describe'],
                capture_output=True,
                text=True,
                timeout=5
            )
            
            if result.returncode == 0:
                return result.stdout
            else:
                return f"Ошибка: {result.stderr}"
                
        except Exception as e:
            return f"Исключение: {e}"
    
    def create_topic_if_missing(self, topic, partitions=3):
        """Идемпотентное создание топика (повторный запуск безопасен)"""
        result = subprocess.run(
            ['/opt/kafka/bin/kafka-topics.sh', '--create',
             '--topic', topic,
             '--bootstrap-server', 'localhost:9092',
             '--partitions', str(partitions),
             '--replication-factor', '3'],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            console.print(f"[green]✅ Топик {topic} создан[/green]")
        elif "already exists" in result.stderr:
            console.print(f"[yellow]ℹ️  Топик {topic} уже существует — продолжаем[/yellow]")
        else:
            console.print(f"[yellow]⚠️  Ошибка создания топика {topic}: {result.stderr.strip()}[/yellow]")
    
    def delete_topic(self, topic):
        """Удаление топика (отсутствующий топик — не ошибка)"""
        result = subprocess.run(
            ['/opt/kafka/bin/kafka-topics.sh', '--delete',
             '--topic', topic,
             '--bootstrap-server', 'localhost:9092'],
            capture_output=True,
            text=True
        )
        combined = (result.stdout + " " + result.stderr).lower()
        if result.returncode == 0 or "does not exist" in combined:
            console.print(f"[green]🗑️  Топик {topic} удалён[/green]")
        else:
            console.print(f"[yellow]⚠️  Не удалось удалить топик {topic}: {result.stderr.strip()}[/yellow]")
    
    def run_scenario(self, scenario_num):
        """Запуск сценария ребалансировки"""
        console.print(f"\n[bold magenta]🔄 СЦЕНАРИЙ {scenario_num}: {self.rebalance_scenarios[scenario_num-1]}[/bold magenta]")
        
        if scenario_num == 1:
            # Сценарий 1: Добавление консюмера
            console.print("1. Запускаем 2 консюмера")
            self.start_consumer('A')
            self.start_consumer('B')
            time.sleep(5)
            
            console.print("\n2. Добавляем третий консюмер")
            self.start_consumer('C')
            time.sleep(5)
            
            console.print("\n3. Проверяем распределение партиций")
            console.print(self.check_consumer_group())
            
        elif scenario_num == 2:
            # Сценарий 2: Удаление консюмера
            console.print("1. Запускаем 3 консюмера")
            self.start_consumer('A')
            self.start_consumer('B') 
            self.start_consumer('C')
            time.sleep(5)
            
            console.print("\n2. Удаляем один консюмер")
            self.stop_consumer('B')
            time.sleep(5)
            
            console.print("\n3. Проверяем перераспределение")
            console.print(self.check_consumer_group())
            
        elif scenario_num == 3:
            # Сценарий 3: Сбой консюмера (kill -9)
            console.print("1. Запускаем 3 консюмера")
            self.start_consumer('A')
            self.start_consumer('B')
            self.start_consumer('C')
            time.sleep(5)
            
            console.print("\n2. Имитируем сбой консюмера B (kill -9)")
            self.kill_consumer('B')
            time.sleep(10)  # Ждем пока Kafka обнаружит сбой
            
            console.print("\n3. Проверяем как Kafka обработала сбой")
            console.print(self.check_consumer_group())
            
        elif scenario_num == 4:
            # Сценарий 4: Увеличение партиций
            console.print("1. Создаем топик test-rebalance с 3 партициями")
            # Идемпотентное создание: повторный запуск не падает с
            # "Topic already exists"
            self.create_topic_if_missing('test-rebalance', partitions=3)
            # Топик будет удалён в конце run_scenario (см. блок очистки ниже)
            self.scenario_topics_to_clean = ['test-rebalance']
            
            console.print("\n2. Запускаем 3 консюмера")
            for i in range(3):
                cmd = [
                    sys.executable, 'manual_commit_consumer.py',
                    '--id', f'rebalance-{i}',
                    '--messages', '500'
                ]
                # Создаем временный скрипт с измененным топиком
                with open('temp_consumer.py', 'w') as f:
                    f.write('''
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from manual_commit_consumer import ManualCommitConsumer
import time

if __name__ == "__main__":
    consumer = ManualCommitConsumer(
        "localhost:9092",
        "manual-commit-group",
        "test-rebalance",
        f'rebalance-{sys.argv[1]}'
    )
    consumer.consume(max_messages=500)
''')
                
                process = subprocess.Popen(
                    [sys.executable, 'temp_consumer.py', str(i)],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )
                self.processes[f'rebalance-{i}'] = process
            
            time.sleep(5)
            
            console.print("\n3. Увеличиваем партиции до 6")
            subprocess.run([
                '/opt/kafka/bin/kafka-topics.sh', '--alter',
                '--topic', 'test-rebalance',
                '--bootstrap-server', 'localhost:9092',
                '--partitions', '6'
            ], capture_output=True)
            
            time.sleep(5)
            
            console.print("\n4. Проверяем новое распределение")
            result = subprocess.run([
                '/opt/kafka/bin/kafka-consumer-groups.sh',
                '--bootstrap-server', 'localhost:9092',
                '--group', 'manual-commit-group',
                '--describe'
            ], capture_output=True, text=True)
            console.print(result.stdout)
            
            # Удаляем временный файл
            if os.path.exists('temp_consumer.py'):
                os.remove('temp_consumer.py')
            
        elif scenario_num == 5:
            # Сценарий 5: Изменение subscription консюмера.
            # Нужны два топика; создаём их идемпотентно (повторный запуск
            # безопасен), в конце run_scenario они будут удалены.
            console.print("1. Создаем топики topic-a и topic-b (по 3 партиции)")
            self.create_topic_if_missing('topic-a', partitions=3)
            self.create_topic_if_missing('topic-b', partitions=3)
            self.scenario_topics_to_clean = ['topic-a', 'topic-b']
            
            console.print("\n2. Запускаем консюмер, подписанный на topic-a")
            # Вспомогательный скрипт: консюмер сначала подписан на topic-a,
            # а примерно через 12 секунд САМ меняет подписку (повторный вызов
            # subscribe()) на topic-b. Kafka проводит ребалансировку: партиции
            # topic-a отзываются, консюмеру назначаются партиции topic-b.
            with open('temp_subscription_switch.py', 'w') as f:
                f.write('''
import sys
import time
from confluent_kafka import Consumer, KafkaError

# Вспомогательный консюмер сценария 5: ~12 секунд подписан на topic-a,
# затем меняет подписку на topic-b (subscribe() можно вызывать повторно —
# старая подписка заменяется новой).
if __name__ == "__main__":
    group_id = sys.argv[1]
    c = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'session.timeout.ms': 10000,
    })
    try:
        c.subscribe(['topic-a'])
        print("Подписан на topic-a")
        deadline = time.time() + 12
        while time.time() < deadline:
            msg = c.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error() and msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"Ошибка: {msg.error()}")

        # Меняем подписку: topic-a -> topic-b
        c.subscribe(['topic-b'])
        print("Подписка изменена на topic-b")
        deadline = time.time() + 20
        while time.time() < deadline:
            msg = c.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error() and msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"Ошибка: {msg.error()}")
    except KeyboardInterrupt:
        pass
    finally:
        c.close()
''')
            self.processes['switch-A'] = subprocess.Popen(
                [sys.executable, 'temp_subscription_switch.py', 'manual-commit-group'],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            console.print(f"[green]✅ Запущен консюмер (PID: {self.processes['switch-A'].pid})[/green]")
            
            # Фаза 1: консюмер ещё на topic-a
            console.print("\n3. Ждём 8 секунд и проверяем распределение (консюмер на topic-a)")
            time.sleep(8)
            console.print("   Ожидаем: партиции topic-a 0-2 назначены консюмеру группы")
            console.print(self.check_consumer_group())
            
            # Фаза 2: консюмер сменил подписку на topic-b (~12-я секунда работы)
            console.print("\n4. Консюмер меняет подписку на topic-b — ждём ребалансировки")
            console.print("   (переключение происходит примерно на 12-й секунде работы консюмера)")
            time.sleep(10)
            console.print("   Ожидаем: партиции topic-a освобождены, назначены партиции topic-b 0-2")
            console.print(self.check_consumer_group())
            
            # Удаляем временный скрипт
            if os.path.exists('temp_subscription_switch.py'):
                os.remove('temp_subscription_switch.py')
            
        # Очистка после сценария: останавливаем консюмеры...
        console.print("\n[yellow]🧹 Очистка...[/yellow]")
        for consumer_id in list(self.processes.keys()):
            self.stop_consumer(consumer_id)
        
        # ...и удаляем топики, созданные сценариями 4-5 (test-rebalance,
        # topic-a, topic-b), чтобы повторные запуски не оставляли мусор
        for topic in self.scenario_topics_to_clean:
            self.delete_topic(topic)
        self.scenario_topics_to_clean = []
        
        time.sleep(2)
    
    def interactive_test(self):
        """Интерактивное тестирование"""
        
        # Проверяем топик
        if not check_topic_exists("user-actions"):
            console.print("[red]❌ Топик 'user-actions' не найден![/red]")
            console.print("[yellow]📌 Запустите сначала подготовку окружения:[/yellow]")
            console.print("[yellow]   python3 setup_demo.py[/yellow]")
            return
        
        console.print("[bold blue]=" * 60)
        console.print("         ИНТЕРАКТИВНОЕ ТЕСТИРОВАНИЕ РЕБАЛАНСИРОВКИ")
        console.print("=" * 60)
        
        while True:
            console.print("\n[bold]Доступные команды:[/bold]")
            console.print("  start <id>    - Запустить консюмер")
            console.print("  stop <id>     - Остановить консюмер (graceful)")
            console.print("  kill <id>     - Убить консюмер (имитация сбоя)")
            console.print("  list          - Список консюмеров")
            console.print("  check         - Проверить consumer group")
            console.print("  scenario <n>  - Запустить сценарий (1-5)")
            console.print("  quit          - Выйти")
            
            try:
                command = input("\nВведите команду: ").strip().split()
                
                if not command:
                    continue
                
                if command[0] == 'start' and len(command) == 2:
                    self.start_consumer(command[1])
                    
                elif command[0] == 'stop' and len(command) == 2:
                    self.stop_consumer(command[1])
                    
                elif command[0] == 'kill' and len(command) == 2:
                    self.kill_consumer(command[1])
                    
                elif command[0] == 'list':
                    console.print("\n[bold]Активные консюмеры:[/bold]")
                    for id, process in self.processes.items():
                        status = "работает" if process.poll() is None else "остановлен"
                        console.print(f"  {id}: PID {process.pid} ({status})")
                    
                elif command[0] == 'check':
                    console.print("\n[bold]Состояние consumer group:[/bold]")
                    console.print(self.check_consumer_group())
                    
                elif command[0] == 'scenario' and len(command) == 2:
                    try:
                        scenario_num = int(command[1])
                        if 1 <= scenario_num <= 5:
                            self.run_scenario(scenario_num)
                        else:
                            console.print("[red]❌ Номер сценария должен быть от 1 до 5[/red]")
                    except ValueError:
                        console.print("[red]❌ Неверный номер сценария[/red]")
                        
                elif command[0] == 'quit':
                    break
                    
                else:
                    console.print("[red]❌ Неизвестная команда[/red]")
                    
            except KeyboardInterrupt:
                console.print("\n[yellow]⚠️  Возврат в меню...[/yellow]")
                continue
            except EOFError:
                break
        
        # Очистка при выходе
        console.print("\n[yellow]🧹 Завершение работы, очистка консюмеров...[/yellow]")
        for consumer_id in list(self.processes.keys()):
            self.stop_consumer(consumer_id)

if __name__ == "__main__":
    tester = RebalanceTester()
    tester.interactive_test()
    console.print("\n[bold green]✅ Тестирование ребалансировки завершено![/bold green]")
