from __future__ import annotations

import asyncio
import json
import os
import time
import logging
from typing import Optional

import aiohttp
from pydantic import BaseModel, ValidationError
from lib.clients.metalake.exceptions import TrinoHttpError, TrinoValidationError, TrinoQueryError
from lib.clients.metalake.models import TrinoQueryPage, TrinoQueryResult, DescribeRow

DEFAULT_BASE = os.environ.get("DEFAULT_BASE") or "http://localhost:6100"
DEFAULT_HEADERS = {"X-Trino-User": "agent", "Content-Type": "text/plain"}
RETRY_STATUS = {429, 502, 503, 504}


def _map_rows_to_model(
    result: TrinoQueryResult,
    model: type[BaseModel],
) -> list[BaseModel]:
    if not result.columns:
        raise ValueError("No columns in result")

    idx = {c.name: i for i, c in enumerate(result.columns)}

    rows = []
    for raw in result.data:
        payload = {
            field: raw[idx[field]]
            for field in model.model_fields
            if field in idx and idx[field] < len(raw)
        }
        rows.append(model.model_validate(payload))
    return rows


class TrinoHttpClient:
    def __init__(
        self,
        base_url: str = DEFAULT_BASE,
        headers: Optional[dict] = None,
        timeout_seconds: float = 60.0,
        max_retries: int = 5,
        backoff_base_ms: int = 100,
        logger: Optional[logging.Logger] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.headers = headers or DEFAULT_HEADERS
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.backoff_base_ms = backoff_base_ms
        self.logger = logger or logging.getLogger(__name__)

    async def _request_json(self, method: str, url: str, data: Optional[str] = None) -> dict:
        timeout_obj = aiohttp.ClientTimeout(total=self.timeout_seconds)
        start = time.time()
        async with aiohttp.ClientSession(timeout=timeout_obj) as session:
            doc, status, reason, retry_after = await self._fetch_json(session, method, url, self.headers, data)
            retries = 0
            while status in RETRY_STATUS and retries < self.max_retries:
                delay_ms = self.backoff_base_ms * (2 ** retries)
                if retry_after:
                    try:
                        delay_ms = int(float(retry_after) * 1000)
                    except Exception:
                        pass
                self.logger.debug(
                    "Retry %s %s %d/%d after %dms (status %d)",
                    method, url, retries + 1, self.max_retries, delay_ms, status
                )
                await asyncio.sleep(delay_ms / 1000.0)
                doc, status, reason, retry_after = await self._fetch_json(session, method, url, self.headers, data)
                retries += 1

            elapsed = time.time() - start
            self.logger.debug("%s %s -> %d %s in %.3fs", method, url, status, reason, elapsed)

            if status >= 400 and status not in {200}:
                preview = doc.get("_raw", "")
                raise TrinoHttpError(method, url, status, reason, preview[:200])

            return doc

    @staticmethod
    async def _fetch_json(
        session: aiohttp.ClientSession,
        method: str,
        url: str,
        headers: dict,
        data: Optional[str] = None
    ) -> tuple[dict, int, str, Optional[str]]:
        async with session.request(method, url, headers=headers, data=data) as resp:
            retry_after = resp.headers.get("Retry-After")
            text = await resp.text()
            try:
                doc = json.loads(text)
            except Exception:
                doc = {"_raw": text}
            return doc, resp.status, resp.reason, retry_after

    async def post_statement(self, sql: str) -> dict:
        url = f"{self.base_url}/v1/statement"
        self.logger.debug("POST %s", url)
        self.logger.debug("Headers: %s", self.headers)
        self.logger.debug("SQL: %s", sql)
        return await self._request_json("POST", url, data=sql)

    async def get_next(self, next_uri: str) -> dict:
        self.logger.debug("GET %s", next_uri)
        return await self._request_json("GET", next_uri)


class TrinoStatementRunner:
    def __init__(self, client: TrinoHttpClient, logger: Optional[logging.Logger] = None):
        self.client = client
        self.logger = logger or client.logger

    async def run(self, sql: str) -> TrinoQueryResult:
        doc = await self.client.post_statement(sql)
        page = self._validate_page(doc, context="first page")
        result = self._init_result(page)

        if page.data:
            self.logger.debug("Page 0 rows: %d (total %d)", len(page.data), len(result.data))

        hop = 0
        next_uri = page.nextUri
        while next_uri:
            hop += 1
            doc = await self.client.get_next(next_uri)
            page = self._validate_page(doc, context=f"hop {hop}")

            if not result.columns and page.columns:
                result.columns = page.columns
            if page.data:
                result.data.extend(page.data)
                self.logger.debug("Page %d rows: %d (total %d)", hop, len(page.data), len(result.data))
            if page.stats:
                result.stats = page.stats
            if page.warnings:
                result.warnings.extend(page.warnings)

            next_uri = page.nextUri

        if doc.get("error"):
            self.logger.debug("Query failed: %s", json.dumps(doc["error"], ensure_ascii=False))
            raise TrinoQueryError(doc["error"].get("message", "Unknown error"), doc["error"])

        self.logger.debug("result stats: %s", json.dumps(result.stats, ensure_ascii=False))
        columns_json = [c.model_dump() for c in result.columns]
        self.logger.debug("Columns: %s", json.dumps(columns_json, ensure_ascii=False))
        self.logger.debug("Total rows returned: %d", len(result.data))
        return result

    @staticmethod
    def _validate_page(doc: dict, context: str) -> TrinoQueryPage:
        try:
            return TrinoQueryPage.model_validate(doc)
        except ValidationError as e:
            raise TrinoValidationError(f"Failed to parse {context}: {e}", doc)

    @staticmethod
    def _init_result(page: TrinoQueryPage) -> TrinoQueryResult:
        return TrinoQueryResult(
            id=page.id,
            infoUri=page.infoUri,
            columns=page.columns or [],
            data=page.data or [],
            stats=page.stats,
            warnings=page.warnings or [],
        )



class TrinoService:
    def __init__(
        self,
        runner: Optional[TrinoStatementRunner] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.logger = logger or logging.getLogger(__name__)
        if runner is None:
            client = TrinoHttpClient(logger=self.logger)
            runner = TrinoStatementRunner(client, logger=self.logger)
        self.runner = runner

    async def execute(self, sql: str) -> TrinoQueryResult:
        self.logger.debug("Execute SQL: %s", sql)
        return await self.runner.run(sql)

    async def describe(self, object_name: str) -> list[DescribeRow]:
        sql = f"DESCRIBE {object_name}"
        self.logger.debug("Describe object: %s", object_name)

        result = await self.runner.run(sql)

        rows = _map_rows_to_model(result, DescribeRow)

        self.logger.debug("Parsed %d describe rows", len(rows))
        return rows
