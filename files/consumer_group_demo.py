#!/usr/bin/env python3
"""
ДЕМОНСТРАЦИЯ РАБОТЫ CONSUMER GROUP С НЕСКОЛЬКИМИ КОНСЮМЕРАМИ

Что демонстрирует этот скрипт:
1. Работу нескольких консюмеров в одной consumer group
2. Автоматическое распределение партиций между консюмерами
3. Ребалансировку при добавлении/удалении консюмеров
4. Мониторинг состояния consumer group через Kafka CLI
5. Параллельную обработку сообщений несколькими консюмерами

Концепции Kafka:
- Consumer Groups и распределение партиций
- Rebalance Protocol (протокол ребалансировки)
- Partition assignment strategies
- Состояния consumer group (Stable, PreparingRebalance, etc.)
- Проверка lag (отставания) консюмеров

Особенности:
- Kafka автоматически распределяет партиции между консюмерами
- При добавлении/удалении консюмера происходит ребалансировка
- Каждая партиция обрабатывается только одним консюмером в группе
- Позволяет масштабировать обработку горизонтально

Запуск:
python3 consumer_group_demo.py
(Выберите вариант 3 для запуска 3 консюмеров)

Результат:
Запустятся 3 консюмера, вы увидите как партиции распределяются между ними.
Можно наблюдать за ребалансировкой при остановке/запуске консюмеров.
"""

import subprocess
import threading
import time
import sys
import os
from rich.console import Console
from rich.table import Table
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel

console = Console()

def check_kafka_topic():
    """Проверка наличия топика"""
    print("🔍 Проверяю топик 'user-actions'...")
    
    try:
        cmd = ['/opt/kafka/bin/kafka-topics.sh', '--describe', 
               '--bootstrap-server', 'localhost:9092', '--topic', 'user-actions']
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
        
        if result.returncode != 0 or "Topic: user-actions" not in result.stdout:
            print("❌ Топик 'user-actions' не найден!")
            print("\n📌 Вам нужно сначала настроить демонстрацию:")
            print("   1. Запустите: python3 setup_demo.py")
            print("   2. Создайте топик и тестовые данные")
            print("   3. Затем запустите эту демонстрацию снова")
            return False
        
        print("✅ Топик 'user-actions' найден")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка при проверке: {e}")
        return False

def run_consumer(consumer_id, num_messages=100):
    """Запуск консюмера в отдельном процессе"""
    cmd = [
        sys.executable, 'manual_commit_consumer.py',
        '--id', consumer_id,
        '--messages', str(num_messages)
    ]
    
    console.print(f"\n[bold green]🚀 Запуск консюмера {consumer_id}[/bold green]")
    
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        universal_newlines=True
    )
    
    # Читаем вывод в фоне, чтобы избежать блокировки
    def read_output(pipe, consumer_id):
        for line in pipe:
            if line.strip():
                console.print(f"[dim][{consumer_id}][/dim] {line.strip()}")
    
    threading.Thread(target=read_output, args=(process.stdout, consumer_id), daemon=True).start()
    threading.Thread(target=read_output, args=(process.stderr, consumer_id), daemon=True).start()
    
    return process

def monitor_consumers(processes):
    """Мониторинг работающих консюмеров"""
    console.print("\n[bold cyan]👁️  Мониторинг консюмеров... (Ctrl+C для остановки)[/bold cyan]")
    console.print("[yellow]ℹ️  Скрипт для экспериментов с ребалансировкой (python3 rebalance_test.py) будет создан в разделе 5 — пока просто наблюдайте за распределением партиций[/yellow]")
    
    try:
        start_time = time.time()
        
        # Простой мониторинг без rich.live
        while True:
            # Проверяем статус каждого процесса
            console.print("\n" + "="*60)
            console.print(f"[bold]Статус консюмеров (время работы: {time.time() - start_time:.1f} сек)[/bold]")
            
            all_running = True
            for consumer_id, process in processes.items():
                returncode = process.poll()
                if returncode is None:
                    status = "🟢 Работает"
                else:
                    status = f"🔴 Завершен (код: {returncode})"
                    all_running = False
                console.print(f"  {consumer_id}: {status}")
            
            if not all_running:
                console.print("\n[yellow]⚠️  Некоторые консюмеры завершились[/yellow]")
                break
            
            # Проверяем consumer group через Kafka
            console.print("\n[bold]Проверка consumer group через Kafka:[/bold]")
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
                    lines = result.stdout.strip().split('\n')
                    # Показываем только первые 5 строк
                    for line in lines[:5]:
                        console.print(f"  {line}")
                    if len(lines) > 5:
                        console.print(f"  ... и ещё {len(lines) - 5} строк")
                else:
                    console.print(f"  [yellow]Не удалось получить информацию: {result.stderr}[/yellow]")
                    
            except Exception as e:
                console.print(f"  [yellow]Ошибка при проверке: {e}[/yellow]")
            
            # Ждем 5 секунд перед следующей проверкой
            time.sleep(5)
            
    except KeyboardInterrupt:
        console.print("\n[yellow]⚠️  Мониторинг прерван пользователем[/yellow]")
    
    finally:
        # Останавливаем все процессы
        console.print("\n[yellow]🛑 Останавливаем все консюмеры...[/yellow]")
        for consumer_id, process in processes.items():
            if process.poll() is None:  # Если ещё работает
                process.terminate()
                try:
                    process.wait(timeout=5)
                    console.print(f"  ✅ {consumer_id}: Остановлен")
                except subprocess.TimeoutExpired:
                    process.kill()
                    console.print(f"  🔴 {consumer_id}: Принудительно завершён")

def check_consumer_group():
    """Проверка consumer group через Kafka CLI"""
    console.print("\n[bold cyan]🔍 Проверка consumer group через Kafka...[/bold cyan]")
    
    try:
        # Получаем список consumer groups
        result = subprocess.run(
            ['/opt/kafka/bin/kafka-consumer-groups.sh',
             '--bootstrap-server', 'localhost:9092',
             '--list'],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        console.print("[bold]Найденные consumer groups:[/bold]")
        for line in result.stdout.strip().split('\n'):
            if line.strip():
                console.print(f"  {line}")
        
        if 'manual-commit-group' in result.stdout:
            console.print("\n[green]✅ Consumer group 'manual-commit-group' найден[/green]")
            
            # Получаем детальную информацию
            result = subprocess.run(
                ['/opt/kafka/bin/kafka-consumer-groups.sh',
                 '--bootstrap-server', 'localhost:9092',
                 '--group', 'manual-commit-group',
                 '--describe'],
                capture_output=True,
                text=True,
                timeout=5
            )
            
            console.print("\n[bold]Информация о consumer group:[/bold]")
            for line in result.stdout.split('\n'):
                if line.strip():
                    console.print(f"  {line}")
        else:
            console.print("\n[yellow]⚠️  Consumer group 'manual-commit-group' не найден (возможно, еще не создан)[/yellow]")
            
    except Exception as e:
        console.print(f"[red]❌ Ошибка при проверке consumer group: {e}[/red]")

def simple_consumer_group_demo():
    """Простая демонстрация работы consumer group"""
    # Проверяем топик
    if not check_kafka_topic():
        return
    
    console.print("\n[bold magenta]🔄 ПРОСТАЯ ДЕМОНСТРАЦИЯ CONSUMER GROUP[/bold magenta]")
    console.print("1. Запускаем 3 консюмера в одной группе")
    console.print("2. Каждому назначатся по 2 партиции (всего 6 партиций в топике 'user-actions')")
    console.print("3. Наблюдаем распределение партиций")
    console.print("4. За ребалансировкой наблюдаем в колонке CONSUMER-ID/HOST вывода ниже")
    console.print("5. (Готовый скрипт экспериментов python3 rebalance_test.py появится в разделе 5)")
    
    # Запускаем 3 консюмера
    processes = {}
    for i in range(1, 4):
        consumer_id = f"consumer-{i}"
        processes[consumer_id] = run_consumer(consumer_id, num_messages=300)
    
    # Даем им поработать 5 секунд
    console.print("\n[yellow]⏳ Консюмеры запускаются (5 секунд)...[/yellow]")
    time.sleep(5)
    
    # Проверяем состояние группы
    check_consumer_group()
    
    # Мониторим их работу
    console.print("\n[yellow]📊 Начинаем мониторинг (нажмите Ctrl+C для остановки)...[/yellow]")
    monitor_consumers(processes)
    
    console.print("\n[bold green]✅ Демонстрация consumer group завершена![/bold green]")

if __name__ == "__main__":
    console.print("[bold blue]=" * 60)
    console.print("         ДЕМОНСТРАЦИЯ CONSUMER GROUP И РЕБАЛАНСИРОВКИ")
    console.print("=" * 60)
    
    # Варианты запуска
    console.print("\n[bold]Варианты демонстрации:[/bold]")
    console.print("  1. Запустить 3 консюмера и наблюдать за ними")
    console.print("  2. Проверить текущее состояние consumer group")
    console.print("  3. Простая демонстрация (рекомендуется)")
    
    try:
        choice = input("\nВыберите вариант (1-3): ").strip()
        
        if choice == '1':
            # Запускаем 3 консюмера
            processes = {}
            for i in range(1, 4):
                consumer_id = f"group-consumer-{i}"
                processes[consumer_id] = run_consumer(consumer_id, num_messages=300)
            
            # Мониторим их работу
            monitor_consumers(processes)
            
        elif choice == '2':
            # Просто проверяем consumer group
            check_consumer_group()
            
        elif choice == '3':
            # Простая демонстрация
            simple_consumer_group_demo()
            
        else:
            console.print("[red]❌ Неверный выбор[/red]")
    
    except KeyboardInterrupt:
        console.print("\n[yellow]⚠️  Прервано пользователем[/yellow]")
    
    console.print("\n[bold green]🎉 Демонстрация завершена![/bold green]")
    console.print("\n[yellow]📚 Дополнительные возможности:[/yellow]")
    console.print("  - Скрипт экспериментов python3 rebalance_test.py появится в разделе 5")
    console.print("  - Скрипт python3 error_handling_consumer.py (обработка ошибок) появится в разделе 4")
    console.print("  - Для просмотра статистики проверьте файлы: dlq_*.json и consumer_stats.json")
