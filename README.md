# Локальный стенд Trino (Cedrus) + Iceberg

Набор контейнеров Trino/Cedrus, MinIO и Postgres для работы с Iceberg объектами и пример реализации Python-клиента с запросами к API

## Стенд: требования, развертывание, доступы
- Что нужно: Docker Compose; для клиента — Python 3.11+ и Java 21 (Temurin: `brew install temurin@21`; если `java` не виден, пропишите `JAVA_HOME` в PATH).
- Рекомендованные ресурсы под Docker Desktop 10 CPU / 12 GB RAM: Cedrus 6 CPU / 9G (heap 7G), Postgres 2 CPU / 2G, MinIO 1 CPU / 1.5G. Настроено в `docker-compose.yml` и `configs/jvm.config`/`configs/config.properties`.
- Каталоги и данные: MinIO бакет `${DWH_BUCKET:-dwh}` хранится в `./data/minio`; метаданные Iceberg в Postgres volume `./data/postgres`; spill Trino в `./data/cedrus-spill`.
- Шаги запуска:
  1) Скопируйте `.env.example` в `.env` и при необходимости поменяйте порты/креды (по умолчанию: Trino 6100, MinIO 6110/6111 `superadmin/superadmin`, Postgres 6120 `superadmin/superadmin`).
  2) `docker compose up -d`. Сеть внутри Compose — `metalake`, фактическое имя в Docker — `dwh-platform-net`.

- TPCH/TPCDS в образе Cedrus: предустановлены каталоги `tpch` и `tpcds` со схемами `sf1`, `sf10`, `sf100`, `sf1000`, `sf10000`, `sf1000000`, `sf300`, `sf3000`, `sf30000`, `tiny` — можно сразу выполнять запросы без загрузки данных.
  - Разница: `tpch` — классический набор ad-hoc аналитики (джойны, агрегаты); `tpcds` — более сложная модель ритейла с сотнями таблиц и разнообразными OLAP-паттернами.
  - Scale factor: `sf1` ≈ 1 GB нежатых данных, `sf10` ≈ 10 GB, `sf100` ≈ 100 GB и т.д.; `tiny` — минимальный учебный размер. Для TPC-DS порядок сопоставим.
  - Пример таблицы — `tpch.sf1.customer` (используется в примере клиента). Для работы с Iceberg-данными используйте каталог `dwh`.

- Веб-интерфейсы (localhost):
  - Trino/Cedrus: `http://localhost:6100` (REST API; UI минимальный, для запросов используйте клиента или DBeaver).
  - MinIO API: `http://localhost:6110` и консоль: `http://localhost:6111` (логин/пароль из `.env`).

- Подключение к Trino из DBeaver:
  - Драйвер: Trino.
  - Host: `localhost`, Port: `6100`, User: любой (например `agent`), без пароля.
  - Catalog: `dwh` (Iceberg), Schema: `default` или нужный namespace.
  - Тестируйте запросом: `SELECT * FROM "dwh"."default"."<table>" LIMIT 5;` или встроенный `tpch.sf1.customer` (если есть каталог tpch).

## Python-клиент
- Код клиента: `lib/clients/metalake` (HTTP-клиент aiohttp + модели pydantic). Методы:
  - `TrinoService.execute(sql)` → `TrinoQueryResult` (`columns`, `data`).
  - `TrinoService.describe(object_name)` → `list[DescribeRow]`.
- Пример использования: `main.py` — делает `DESCRIBE tpch.sf1.customer` и `SELECT * FROM tpch.sf1.customer LIMIT 5`.
- Шаги запуска Python-клиента (после `docker compose up -d`):
  1) Создайте окружение и активируйте: `python -m venv .venv && source .venv/bin/activate`.
  2) Установите зависимости: `pip install -r requirements.txt`.
  3) Запустите пример: `python main.py`.
  4) При необходимости переопределите базовый URL: `DEFAULT_BASE=http://localhost:6100 python main.py` (по умолчанию `http://localhost:6100`, заголовок пользователя — `X-Trino-User: agent`).
- Логирование: через `setup_logging(level=INFO|DEBUG|WARNING|ERROR)`; уровень `DEBUG` показывает сетевые вызовы и статистику страниц/результатов.

Postgres выполняет `scripts/init_pg.sql` только при первичной инициализации volume `./data/postgres`. Если чистили данные, убедитесь, что база `metalake` и объекты Iceberg созданы (при необходимости повторите скрипт).
