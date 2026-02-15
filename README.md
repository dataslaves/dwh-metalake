# Локальный стенд Trino (Cedrus) + Iceberg

Набор контейнеров Trino/Cedrus, MinIO и Postgres для работы с Iceberg-таблицами и простым Python-клиентом.

## Что нужно
- Docker Compose
- Python 3.11+
- Java 21 (Temurin: `brew install temurin@21`). Если `java` не виден, добавьте в профиль `export JAVA_HOME=$(/usr/libexec/java_home -v 21); export PATH="$JAVA_HOME/bin:$PATH"`.

## Запуск
1. Скопируйте `.env.example` в `.env` и при необходимости поменяйте порты/креды. По умолчанию: Trino 6100, MinIO 6110/6111 (`superadmin/superadmin`), Postgres 6120 (`superadmin/superadmin`).
2. Поднимите сервисы: `docker compose up -d`. Данные лежат в `./data/minio` и `./data/postgres`. В Compose сеть называется `metalake`, фактическое имя в Docker — `dwh-platform-net`.
3. Создайте venv: `python -m venv .venv && source .venv/bin/activate` (Windows: `.venv\Scripts\activate`).
4. Установите зависимости: `pip install --upgrade pip && pip install -r requirements.txt`.
5. Запустите пример: `python main.py` — увидите DESCRIBE и выборку из Iceberg.

## Рекомендованные ресурсы (под Docker Desktop 10 CPU / 12 GB RAM)
- Cedrus/Trino: `cpus: 6`, `mem_limit: 9g`, `mem_reservation: 5g`. Heap в `configs/jvm.config`: `-Xmx7G`, `-Xms7G`.
- Trino memory (`configs/config.properties`): `query.max-memory=5GB`, `query.max-memory-per-node=4GB`, `memory.heap-headroom-per-node=1GB`, `task.concurrency=8`.
- Postgres: `cpus: 2`, `mem_limit: 2g`, `mem_reservation: 1g`; базовые параметры заданы в `docker-compose.yml` (`shared_buffers`, `work_mem`, `effective_cache_size`, `checkpoint_timeout`, `max_wal_size`).
- MinIO: `cpus: 1`, `mem_limit: 1536m`, `mem_reservation: 1g` — хватит для тестов; при высокой загрузке увеличивайте до 2 CPU / 3–4 GB.
- Spill Trino: volume `./data/cedrus-spill` → `/var/trino/spill`; следите за диском хоста. При активных мелких запросах можно переключить на быстрый диск или tmpfs.

## Клиент
- `TrinoService.execute(sql)` возвращает `TrinoQueryResult` с `columns` и `data`.
- `TrinoService.describe(object_name)` возвращает список `DescribeRow` (pydantic).

## Логи
- `setup_logging(level=INFO|DEBUG|WARNING|ERROR)`.
- `DEBUG` включает сетевые вызовы и статистику выполнения.

## Конфиги Trino
- `configs/config.properties` и `configs/dwh.properties` используют подстановку `${ENV:VAR}` из окружения контейнера (значения берутся из `.env`).
- Дополнительная генерация файлов не требуется — Trino сам подставляет значения при старте.

Postgres запускает `scripts/init_pg.sql` только при первой инициализации volume `./data/postgres`. После очистки данных убедитесь, что база `metalake` и объекты Iceberg созданы (повторите скрипт при необходимости).
