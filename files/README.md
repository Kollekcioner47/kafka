# Готовые скрипты к лабораторным 2.2 и 2.3

Эта папка содержит все Python-скрипты из лабораторных работ 2.2 (продюсеры)
и 2.3 (консюмеры) — извлечены из текста лаб без изменений. Текст лабораторных
(heredoc-вставки) НЕ изменялся: студент может либо скопировать скрипты из лабы,
либо скопировать готовые файлы из этой папки в рабочий каталог.

## Куда класть файлы

Все скрипты обеих лаб работают в одном каталоге `~/kafka-lab/producers`
(он создаётся в лабе 2.2). Скопируйте нужные файлы туда и активируйте venv:

    mkdir -p ~/kafka-lab/producers
    cp files/*.py ~/kafka-lab/producers/
    cd ~/kafka-lab/producers
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt

## Зависимости (requirements.txt)

- `confluent-kafka` — официальный Python-клиент (нужен ВСЕМ скриптам)
- `rich` — красивый вывод таблиц/прогресса (нужен почти всем)
- `faker` — генерация имён (только basic_producer.py)

## Лабораторная 2.2 — продюсеры

| Файл | Что делает |
|---|---|
| `basic_producer.py` | Сравнение acks=0/1/all, замер throughput |
| `retry_producer.py` | Ретраи библиотеки + DLQ-топик `test-dlq` при фатальной ошибке |
| `async_producer.py` | Асинхронная отправка + замер latency (avg/p95) |
| `performance_test.py` | Сравнение 5 конфигураций, таблица результатов + CSV |
| `verify_production.py` | Подсчёт дошедших сообщений (читает test-producer и test-perf) |

Топики: `test-producer`, `test-perf`, `test-dlq`.

## Лабораторная 2.3 — консюмеры

| Файл | Что делает |
|---|---|
| `setup_demo.py` | Создаёт топик `user-actions` (6 партиций, RF=3) + 5000 сообщений |
| `basic_consumer.py` | Автокоммит, чтение 100 сообщений |
| `manual_commit_consumer.py` | Ручной коммит, retry, DLQ-файл, rebalance-колбэки |
| `consumer_group_demo.py` | Запускает 3 консюмера в группе, мониторит распределение |
| `error_handling_consumer.py` | Валидация, дедупликация, Circuit Breaker, DLQ-топик |
| `rebalance_test.py` | 5 интерактивных сценариев ребалансировки |
| `simple_visual_consumer.py` | Минимальный консюмер для визуализации |
| `visual_demo_2_3.py` | Пошаговая демонстрация ребалансировки |

Группы: `basic-consumer-group`, `manual-commit-group`, `error-handling-group`,
`visual-demo-group`. Топики: `user-actions`, `user-actions-error`,
`user-actions-error-dlq`.

## Порядок запуска (лаб 2.3)

    python3 setup_demo.py            # 1. данные
    python3 basic_consumer.py        # 2. автокоммит
    python3 manual_commit_consumer.py --id consumer-1 --messages 50
    python3 consumer_group_demo.py   # выбрать вариант 3
    python3 error_handling_consumer.py --produce --messages 30
    python3 rebalance_test.py        # интерактивно: scenario 1..5

## Примечание о retry/DLQ в демо

- `manual_commit_consumer.py`: симулированная ошибка срабатывает только на 1-й
  попытке (со 2-й всегда успех) → ветка DLQ-файла на практике НЕ выполняется.
- `error_handling_consumer.py`: retry-путь (RETRY) не воспроизводится — «случайные»
  ошибки уходят сразу в DLQ, а не в retry; Circuit Breaker не открывается
  (нужно ≥10 ошибок, а в демо их ~5).
См. детали в отчёте по соответствию заявленного и фактического поведения.
