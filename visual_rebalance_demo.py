#!/usr/bin/env python3
"""
ВИЗУАЛЬНАЯ ДЕМОНСТРАЦИЯ РЕБАЛАНСИРОВКИ KAFKA - РАБОЧАЯ ВЕРСИЯ
"""

import subprocess
import time
import threading
from datetime import datetime

def print_header(text):
    """Красивый заголовок"""
    print("\n" + "═" * 80)
    print(f"  {text}")
    print("═" * 80)

def print_step(step, description):
    """Шаг демонстрации"""
    print(f"\n📌 Шаг {step}: {description}")
    print("-" * 60)

def run_kafka_command(cmd, timeout=5):
    """Выполнение команды Kafka"""
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout
        )
        return result.stdout if result.returncode == 0 else result.stderr
    except Exception as e:
        return f"Ошибка: {e}"

def get_consumer_group_state():
    """Получение состояния consumer group"""
    cmd = [
        '/opt/kafka/bin/kafka-consumer-groups.sh',
        '--bootstrap-server', 'localhost:9092',
        '--group', 'visual-demo-group',
        '--describe'
    ]
    
    output = run_kafka_command(cmd, timeout=10)
    return output

def print_consumer_group_info():
    """Вывод информации о consumer group"""
    print("\n📊 ИНФОРМАЦИЯ О CONSUMER GROUP:")
    
    info = get_consumer_group_state()
    if not info or "Error" in info:
        print("  ❌ Consumer group не найден или ошибка запроса")
        print(f"  Детали: {info[:200]}")
        return {}
    
    print(info)
    
    # Парсим для визуализации
    lines = info.strip().split('\n')
    if len(lines) < 2:
        return {}
    
    # Парсим таблицу
    consumers = {}
    for line in lines[1:]:  # Пропускаем заголовок
        if not line.strip():
            continue
        
        parts = line.split()
        if len(parts) >= 6:
            consumer_id = parts[1]
            partitions = parts[5].split(',')
            consumers[consumer_id] = partitions
    
    return consumers

def start_demo_consumer(name):
    """Запуск демо-консюмера"""
    print(f"  🚀 Запускаем демо-консюмер: {name}")
    
    cmd = [
        'python3', 'demo_consumer.py',
        '--id', name,
        '--group', 'visual-demo-group'
    ]
    
    # Запускаем в фоне
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        universal_newlines=True
    )
    
    # Функция для чтения вывода
    def read_output(pipe, consumer_name):
        for line in pipe:
            if line.strip():
                print(f"    [{consumer_name}] {line.strip()}")
    
    # Запускаем потоки для чтения вывода
    threading.Thread(target=read_output, args=(process.stdout, name), daemon=True).start()
    threading.Thread(target=read_output, args=(process.stderr, name), daemon=True).start()
    
    return process

def wait_and_check(duration=5, message="Ждем..."):
    """Ожидание и проверка состояния"""
    print(f"\n⏳ {message} ({duration} секунд)")
    for i in range(duration, 0, -1):
        print(f"  {i}...", end=' ', flush=True)
        time.sleep(1)
    print()
    
    return print_consumer_group_info()

def stop_consumer(process, name):
    """Остановка консюмера"""
    print(f"\n🛑 Останавливаем консюмер {name}...")
    process.terminate()
    try:
        process.wait(timeout=5)
        print(f"  ✅ Консюмер {name} остановлен")
    except subprocess.TimeoutExpired:
        process.kill()
        print(f"  ⚠️  Консюмер {name} принудительно завершен")

def scenario_1_basic_rebalance():
    """Базовый сценарий ребалансировки"""
    print_header("БАЗОВАЯ ДЕМОНСТРАЦИЯ РЕБАЛАНСИРОВКИ")
    
    print("🎯 Цель: показать, как партиции распределяются между консюмерами")
    
    processes = {}
    
    # Шаг 1: Проверяем начальное состояние
    print_step(1, "НАЧАЛЬНОЕ СОСТОЯНИЕ")
    print_consumer_group_info()
    
    input("\n⏎ Нажмите Enter, чтобы запустить первого консюмера...")
    
    # Шаг 2: Запускаем первого консюмера
    print_step(2, "ЗАПУСК ПЕРВОГО КОНСЮМЕРА")
    print("\n📖 Теория:")
    print("   • Запускаем консюмер consumer-1")
    print("   • Ему назначатся ВСЕ партиции топика (6 партиций)")
    print("   • Нагрузка не распределена - все на одном консюмере")
    
    processes['consumer-1'] = start_demo_consumer('consumer-1')
    consumers = wait_and_check(8, "Ждем, пока consumer-1 подключится и получит партиции")
    
    print("\n✅ Результат: Consumer-1 получил все партиции")
    
    input("\n⏎ Нажмите Enter, чтобы запустить второго консюмера...")
    
    # Шаг 3: Запускаем второго консюмера
    print_step(3, "ЗАПУСК ВТОРОГО КОНСЮМЕРА - НАЧАЛО РЕБАЛАНСИРОВКИ")
    print("\n📖 Теория:")
    print("   • Запускаем consumer-2")
    print("   • Kafka замечает нового консюмера")
    print("   • Начинается АВТОМАТИЧЕСКАЯ РЕБАЛАНСИРОВКА")
    print("   • Партиции перераспределяются между консюмерами")
    
    print("\n⚡ ОЖИДАЕМ РЕБАЛАНСИРОВКУ...")
    processes['consumer-2'] = start_demo_consumer('consumer-2')
    
    # Даем время для ребалансировки
    time.sleep(3)
    print("\n🔄 Идет ребалансировка...")
    
    consumers = wait_and_check(7, "Ждем завершения ребалансировки")
    
    print("\n✅ Результат: Партиции равномерно распределены между consumer-1 и consumer-2")
    if consumers:
        print("\n📈 РАСПРЕДЕЛЕНИЕ ПОСЛЕ РЕБАЛАНСИРОВКИ:")
        for consumer, partitions in consumers.items():
            print(f"  {consumer}: {len(partitions)} партиций - {partitions}")
    
    input("\n⏎ Нажмите Enter, чтобы запустить третьего консюмера...")
    
    # Шаг 4: Запускаем третьего консюмера
    print_step(4, "ЗАПУСК ТРЕТЬЕГО КОНСЮМЕРА")
    print("\n📖 Теория:")
    print("   • Добавляем consumer-3")
    print("   • Снова происходит ребалансировка")
    print("   • Теперь 6 партиций делятся на 3 консюмера")
    print("   • Идеально: по 2 партиции на каждого")
    
    processes['consumer-3'] = start_demo_consumer('consumer-3')
    consumers = wait_and_check(8, "Ждем вторую ребалансировку")
    
    print("\n✅ Результат: Теперь 3 консюмера, каждый обрабатывает по 2 партиции")
    
    input("\n⏎ Нажмите Enter, чтобы остановить одного консюмера...")
    
    # Шаг 5: Останавливаем одного консюмера
    print_step(5, "ОСТАНОВКА CONSUMER-2 - ЕЩЁ ОДНА РЕБАЛАНСИРОВКА")
    print("\n📖 Теория:")
    print("   • Останавливаем consumer-2 (graceful shutdown)")
    print("   • Kafka замечает, что консюмер отключился")
    print("   • Его партиции перераспределяются между оставшимися консюмерами")
    
    stop_consumer(processes['consumer-2'], 'consumer-2')
    del processes['consumer-2']
    
    consumers = wait_and_check(8, "Ждем ребалансировку после остановки consumer-2")
    
    print("\n✅ Результат: Партиции consumer-2 распределены между consumer-1 и consumer-3")
    
    # Заключение
    print_step(6, "ИТОГИ ДЕМОНСТРАЦИИ")
    
    print("\n🎓 ЧТО МЫ УВИДЕЛИ:")
    print("   1. Kafka автоматически распределяет партиции между консюмерами")
    print("   2. При добавлении нового консюмера происходит ребалансировка")
    print("   3. При удалении консюмера также происходит ребалансировка")
    print("   4. Цель Kafka - равномерно распределить нагрузку")
    
    print("\n⚙️  КАК ЭТО РАБОТАЕТ:")
    print("   • Каждые session.timeout.ms консюмеры отправляют heartbeat")
    print("   • Если консюмер не отвечает, он считается 'мертвым'")
    print("   • Тогда начинается ребалансировка")
    print("   • Вся группа останавливается, партиции перераспределяются")
    
    print("\n⚠️  ВАЖНО:")
    print("   • Во время ребалансировки обработка сообщений приостанавливается")
    print("   • Слишком частые ребалансировки - это проблема (rebalance storms)")
    print("   • Надо настраивать session.timeout.ms и max.poll.interval.ms")
    
    # Очистка
    print("\n🧹 Завершение демонстрации, останавливаем всех консюмеров...")
    for name, process in list(processes.items()):
        stop_consumer(process, name)
    
    print("\n" + "═" * 80)
    print("✅ ДЕМОНСТРАЦИЯ ЗАВЕРШЕНА!")
    print("═" * 80)

def interactive_experiment():
    """Интерактивный эксперимент"""
    print_header("ИНТЕРАКТИВНЫЙ ЭКСПЕРИМЕНТ")
    
    print("🎯 Управляйте консюмерами вручную и наблюдайте за ребалансировкой")
    
    print("\n📖 КОМАНДЫ:")
    print("  start <имя>  - запустить консюмера")
    print("  stop <имя>   - остановить консюмера")
    print("  status       - показать состояние группы")
    print("  help         - показать справку")
    print("  exit         - выйти")
    
    processes = {}
    
    while True:
        try:
            command = input("\n🧪 experiment> ").strip().split()
            
            if not command:
                continue
            
            if command[0] == 'start' and len(command) == 2:
                name = command[1]
                if name in processes:
                    print(f"❌ Консюмер {name} уже запущен")
                else:
                    processes[name] = start_demo_consumer(name)
                    print(f"✅ Консюмер {name} запущен")
                    print("   ⏳ Подождите 5 секунд, затем проверьте status")
            
            elif command[0] == 'stop' and len(command) == 2:
                name = command[1]
                if name in processes:
                    stop_consumer(processes[name], name)
                    del processes[name]
                else:
                    print(f"❌ Консюмер {name} не найден")
            
            elif command[0] == 'status':
                print("\n" + "═" * 60)
                print("ТЕКУЩЕЕ СОСТОЯНИЕ CONSUMER GROUP")
                print("═" * 60)
                consumers = print_consumer_group_info()
                
                if consumers:
                    print("\n📈 ВИЗУАЛИЗАЦИЯ РАСПРЕДЕЛЕНИЯ:")
                    total = sum(len(p) for p in consumers.values())
                    for consumer, partitions in consumers.items():
                        bar = "█" * len(partitions) * 2
                        print(f"  {consumer}: {bar} ({len(partitions)} партиций)")
                else:
                    print("\n⚠️  Нет активных консюмеров в группе")
            
            elif command[0] == 'help':
                print("\n📖 СПРАВКА ПО ЭКСПЕРИМЕНТУ:")
                print("1. Запустите консюмеров: start consumer1, start consumer2")
                print("2. Проверьте распределение: status")
                print("3. Добавьте ещё консюмеров: start consumer3")
                print("4. Проверьте, как изменилось распределение: status")
                print("5. Остановите одного: stop consumer1")
                print("6. Снова проверьте status")
                print("\n🎯 Наблюдайте за автоматической ребалансировкой!")
            
            elif command[0] == 'exit':
                print("\n🧹 Очистка...")
                for name, process in list(processes.items()):
                    stop_consumer(process, name)
                break
            
            else:
                print("❌ Неизвестная команда. Введите 'help' для справки")
                
        except KeyboardInterrupt:
            print("\n\n🧹 Прерывание... очистка консюмеров")
            for name, process in list(processes.items()):
                stop_consumer(process, name)
            break
        except Exception as e:
            print(f"❌ Ошибка: {e}")

def main():
    """Главное меню"""
    print_header("ВИЗУАЛЬНАЯ ДЕМОНСТРАЦИЯ РЕБАЛАНСИРОВКИ KAFKA")
    
    print("\n🎯 Эта демонстрация покажет:")
    print("   • Как партиции распределяются между консюмерами")
    print("   • Что происходит при добавлении нового консюмера")
    print("   • Что происходит при остановке консюмера")
    print("   • Как работает автоматическая ребалансировка")
    
    while True:
        print("\n" + "="*80)
        print("ВЫБЕРИТЕ РЕЖИМ:")
        print("  1. 📚 Автоматическая демонстрация (рекомендуется для первого раза)")
        print("  2. 🧪 Интерактивный эксперимент (самостоятельное управление)")
        print("  3. 🚪 Выход")
        print("="*80)
        
        choice = input("\nВаш выбор (1-3): ").strip()
        
        if choice == '1':
            scenario_1_basic_rebalance()
        elif choice == '2':
            interactive_experiment()
        elif choice == '3':
            print("\n👋 До свидания!")
            break
        else:
            print("\n❌ Неверный выбор. Попробуйте снова.")

if __name__ == "__main__":
    try:
        # Проверяем, что Kafka доступна
        print("🔍 Проверяем подключение к Kafka...")
        test_cmd = ['/opt/kafka/bin/kafka-topics.sh', '--list', '--bootstrap-server', 'localhost:9092']
        result = subprocess.run(test_cmd, capture_output=True, text=True, timeout=5)
        
        if result.returncode == 0:
            print("✅ Подключение к Kafka успешно")
            main()
        else:
            print("❌ Не удалось подключиться к Kafka")
            print(f"Ошибка: {result.stderr}")
            
    except KeyboardInterrupt:
        print("\n\n👋 Демонстрация прервана")
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
