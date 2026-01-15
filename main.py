from __future__ import annotations

import asyncio
import logging
from typing import Any

from lib.clients.metalake.core import TrinoService


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    logging.basicConfig(
        level=level,
        format="%(levelname)s %(name)s: %(message)s"
    )
    app_logger = logging.getLogger("app.trino")
    app_logger.setLevel(level)
    return app_logger


def rows_to_dicts(columns: list[Any], data: list[list[Any]]) -> list[dict[str, Any]]:
    column_names = [c.name for c in columns]
    result = []
    for row in data:
        limit = min(len(column_names), len(row))
        result.append({column_names[i]: row[i] for i in range(limit)})
    return result


async def main() -> None:
    logger = setup_logging(level=logging.DEBUG)
    service = TrinoService(logger=logger)

    try:
        describe_rows = await service.describe("tpch.sf1.customer")
        print("Describe rows:")
        for r in describe_rows:
            print(r.model_dump())

        select_result = await service.execute("SELECT * FROM tpch.sf1.customer LIMIT 5")
        rows_as_dicts = rows_to_dicts(select_result.columns, select_result.data)
        print("\nSelect rows:")
        for r in rows_as_dicts:
            print(r)

    except Exception as e:
        logger.exception("Operation failed: %s", e)


if __name__ == "__main__":
    asyncio.run(main())
