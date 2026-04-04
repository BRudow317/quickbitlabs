from __future__ import annotations

import asyncio
import io
import json
import datetime
import math
from collections.abc import AsyncIterator
from typing import Any, TYPE_CHECKING, TypedDict

import pyarrow as pa
import pyarrow.csv as pa_csv

from server.plugins.sf.SfModels import (
    Operation, JobState, ColumnDelimiter, LineEnding,
    ResultsType
)
from server.plugins.sf.utils.filter_null_bytes import filter_null_bytes

if TYPE_CHECKING:
    from server.plugins.sf.SfClient import SfClient

import logging
logger = logging.getLogger(__name__)

# Bulk 2.0 constants - moved here from csv_utils since that module is no longer needed
MAX_INGEST_JOB_FILE_SIZE  = 150 * 1024 * 1024   # 150 MB per job
MAX_INGEST_JOB_PARALLELISM = 15 # SF concurrent job limit
DEFAULT_QUERY_PAGE_SIZE    = 50_000


class QueryBytesResult(TypedDict):
    locator: str
    number_of_records: int
    data: bytes  # raw CSV bytes - caller parses with PyArrow using the object schema


def _arrow_to_csv_bytes(table: pa.Table, line_ending: LineEnding = LineEnding.LF) -> bytes:
    """
    Serialize an Arrow table to CSV bytes in a BytesIO buffer.
    Never touches disk. Line ending matches what was declared in the job creation payload.
    """
    buf = io.BytesIO()
    pa_csv.write_csv(table, buf, write_options=pa_csv.WriteOptions(include_header=True))
    data = buf.getvalue()
    # PyArrow writes LF by default - convert if job declared CRLF
    if line_ending == LineEnding.CRLF:
        data = data.replace(b"\n", b"\r\n")
    return data


######################################################################

class bulk2:
    """
    Entry point for Bulk 2.0 operations.
    Usage: sf.bulk2.Contact.insert(table)
           sf.bulk2.query("SELECT Id FROM Contact")
    """
    _http: SfClient
    bulk2_url: str

    def __init__(self, http_client: SfClient) -> None:
        self._http = http_client
        self.bulk2_url = f"{self._http.services_url}/jobs/"

    def __getattr__(self, name: str) -> SfBulk2Type:
        if name.startswith("__"):
            return super().__getattribute__(name)
        return SfBulk2Type(object_name=name, bulk2_url=self.bulk2_url, http_client=self._http)

    async def query(self, soql: str, **kwargs: Any) -> AsyncIterator[bytes]:
        async for chunk in SfBulk2Type("_query", self.bulk2_url, self._http).query(soql, **kwargs):
            yield chunk

    async def query_all(self, soql: str, **kwargs: Any) -> AsyncIterator[bytes]:
        async for chunk in SfBulk2Type("_query", self.bulk2_url, self._http).query_all(soql, **kwargs):
            yield chunk


######################################################################

class SfBulk2Type:
    """High-level Bulk 2.0 interface for a specific SObject."""
    object_name: str
    bulk2_url: str
    _http: SfClient
    _client: _Bulk2Client

    def __init__(self, object_name: str, bulk2_url: str, http_client: SfClient) -> None:
        self.object_name = object_name
        self.bulk2_url = bulk2_url
        self._http = http_client
        self._client = _Bulk2Client(object_name, bulk2_url, http_client)

    # --- Ingest internals ---

    @staticmethod
    def _split_table(table: pa.Table, chunk_size: int | None) -> list[pa.Table]:
        """
        Split an Arrow table into row-count chunks for upload.
        If chunk_size is None, estimates rows based on the 150MB job limit.
        """
        size = chunk_size or (MAX_INGEST_JOB_FILE_SIZE // 500)  # ~500 bytes/row estimate
        return [table.slice(i, size) for i in range(0, len(table), size)]

    async def _upload_chunk(
        self,
        operation: Operation,
        data: bytes,
        record_count: int,
        column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
        line_ending: LineEnding = LineEnding.LF,
        external_id_field: str | None = None,
        wait: int = 5,
    ) -> dict[str, int]:
        """Upload a single in-memory CSV bytes buffer as one Bulk 2.0 job."""
        res = await self._client.create_job(
            operation,
            column_delimiter=column_delimiter,
            line_ending=line_ending,
            external_id_field=external_id_field,
        )
        job_id = res["id"]

        try:
            if res["state"] != JobState.open.value:
                raise Exception(f"Job {job_id} created in unexpected state: {res['state']}")

            await self._client.upload_job_data(job_id, data)
            await self._client.close_job(job_id)
            await self._client.wait_for_job(job_id, is_query=False, wait=wait)
            res = await self._client.get_job(job_id, is_query=False)

            return {
                "numberRecordsFailed":   int(res["numberRecordsFailed"]),
                "numberRecordsProcessed": int(res["numberRecordsProcessed"]),
                "numberRecordsTotal":    record_count,
                "job_id":                job_id,
            }

        except Exception:
            # Best-effort abort on failure - don't swallow the original exception
            try:
                current = await self._client.get_job(job_id, is_query=False)
                if current["state"] in (
                    JobState.upload_complete.value,
                    JobState.in_progress.value,
                    JobState.open.value,
                ):
                    await self._client.abort_job(job_id, is_query=False)
            except Exception as abort_err:
                logger.warning(f"Failed to abort job {job_id} during cleanup: {abort_err}")
            raise

    async def _upload_table(
        self,
        operation: Operation,
        table: pa.Table,
        chunk_size: int | None = None,
        column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
        line_ending: LineEnding = LineEnding.LF,
        external_id_field: str | None = None,
        concurrency: int = 1,
        wait: int = 5,
    ) -> list[dict[str, int]]:
        """
        Serialize each Arrow table chunk to CSV bytes in memory and upload
        as Bulk 2.0 jobs. Concurrent uploads via asyncio.gather.
        CSV bytes never written to disk.
        """
        chunks = self._split_table(table, chunk_size)
        workers = min(concurrency, MAX_INGEST_JOB_PARALLELISM)
        results: list[dict[str, int]] = []

        if workers == 1:
            for chunk in chunks:
                result = await self._upload_chunk(
                    operation, _arrow_to_csv_bytes(chunk, line_ending), len(chunk),
                    column_delimiter, line_ending, external_id_field, wait,
                )
                results.append(result)
            return results

        # Batch concurrent uploads - respect the parallelism ceiling
        pending = [
            self._upload_chunk(
                operation, _arrow_to_csv_bytes(chunk, line_ending), len(chunk),
                column_delimiter, line_ending, external_id_field, wait,
            )
            for chunk in chunks
        ]
        for i in range(0, len(pending), workers):
            batch_results = await asyncio.gather(*pending[i : i + workers])
            results.extend(batch_results)
        return results

    # --- Public ingest operations ---

    async def insert(
        self,
        table: pa.Table,
        chunk_size: int | None = None,
        concurrency: int = 1,
        line_ending: LineEnding = LineEnding.LF,
        wait: int = 5,
    ) -> list[dict[str, int]]:
        return await self._upload_table(
            Operation.insert, table,
            chunk_size=chunk_size, concurrency=concurrency,
            line_ending=line_ending, wait=wait,
        )

    async def upsert(
        self,
        table: pa.Table,
        external_id_field: str,
        chunk_size: int | None = None,
        line_ending: LineEnding = LineEnding.LF,
        wait: int = 5,
    ) -> list[dict[str, int]]:
        return await self._upload_table(
            Operation.upsert, table,
            chunk_size=chunk_size, line_ending=line_ending,
            external_id_field=external_id_field, wait=wait,
        )

    async def update(
        self,
        table: pa.Table,
        chunk_size: int | None = None,
        line_ending: LineEnding = LineEnding.LF,
        wait: int = 5,
    ) -> list[dict[str, int]]:
        return await self._upload_table(
            Operation.update, table,
            chunk_size=chunk_size, line_ending=line_ending, wait=wait,
        )

    async def delete(
        self,
        table: pa.Table,
        line_ending: LineEnding = LineEnding.LF,
        wait: int = 5,
    ) -> list[dict[str, int]]:
        self._constrain_id_only(table)
        return await self._upload_table(
            Operation.delete, table, line_ending=line_ending, wait=wait,
        )

    async def hard_delete(
        self,
        table: pa.Table,
        line_ending: LineEnding = LineEnding.LF,
        wait: int = 5,
    ) -> list[dict[str, int]]:
        self._constrain_id_only(table)
        return await self._upload_table(
            Operation.hard_delete, table, line_ending=line_ending, wait=wait,
        )

    @staticmethod
    def _constrain_id_only(table: pa.Table) -> None:
        """Delete and hard_delete payloads must contain only the Id column."""
        if [c.lower() for c in table.column_names] != ["id"]:
            raise Exception(
                f"Delete operations require a table with only an 'Id' column. "
                f"Got: {table.column_names}"
            )

    # --- Query operations ---

    async def query(
        self,
        query: str,
        max_records: int = DEFAULT_QUERY_PAGE_SIZE,
        line_ending: LineEnding = LineEnding.LF,
        wait: int = 5,
    ) -> AsyncIterator[bytes]:
        """
        Async generator - yields raw CSV bytes per page.
        Caller parses each page with PyArrow using the cached object schema.
        """
        res = await self._client.create_job(
            Operation.query, query=query, line_ending=line_ending
        )
        job_id = res["id"]
        await self._client.wait_for_job(job_id, is_query=True, wait=wait)

        locator = ""
        first = True
        while first or locator:
            first = False
            result = await self._client.get_query_results(job_id, locator, max_records)
            locator = result["locator"]
            yield result["data"]

    async def query_all(
        self,
        query: str,
        max_records: int = DEFAULT_QUERY_PAGE_SIZE,
        line_ending: LineEnding = LineEnding.LF,
        wait: int = 5,
    ) -> AsyncIterator[bytes]:
        """Includes soft-deleted records. Same yield contract as query()."""
        res = await self._client.create_job(
            Operation.query_all, query=query, line_ending=line_ending
        )
        job_id = res["id"]
        await self._client.wait_for_job(job_id, is_query=True, wait=wait)

        locator = ""
        first = True
        while first or locator:
            first = False
            result = await self._client.get_query_results(job_id, locator, max_records)
            locator = result["locator"]
            yield result["data"]

    # --- Ingest result retrieval ---

    async def get_successful_records(self, job_id: str) -> bytes:
        return await self._client.get_ingest_results(job_id, ResultsType.successful.value)

    async def get_failed_records(self, job_id: str) -> bytes:
        return await self._client.get_ingest_results(job_id, ResultsType.failed.value)

    async def get_unprocessed_records(self, job_id: str) -> bytes:
        return await self._client.get_ingest_results(job_id, ResultsType.unprocessed.value)

    async def get_all_ingest_results(self, job_id: str) -> dict[str, bytes]:
        """
        Fetch all three result sets concurrently.
        Returns raw bytes for each - parse with PyArrow using the object schema.
        """
        successful, failed, unprocessed = await asyncio.gather(
            self.get_successful_records(job_id),
            self.get_failed_records(job_id),
            self.get_unprocessed_records(job_id),
        )
        return {
            "successfulRecords":   successful,
            "failedRecords":       failed,
            "unprocessedRecords":  unprocessed,
        }


######################################################################

class _Bulk2Client:
    """Low-level Bulk 2.0 HTTP operations. Not for direct use outside SfBulk2Type."""

    JSON_CONTENT_TYPE         = "application/json"
    CSV_CONTENT_TYPE          = "text/csv; charset=UTF-8"
    DEFAULT_WAIT_TIMEOUT_SECONDS = 7800  # 6 hours
    MAX_CHECK_INTERVAL_SECONDS   = 60.0

    def __init__(self, object_name: str, bulk2_url: str, http_client: SfClient) -> None:
        self.object_name = object_name
        self.bulk2_url = bulk2_url
        self._http = http_client

    def _headers(self, content_type: str | None = None, accept: str | None = None) -> dict[str, str]:
        return {
            "Content-Type": content_type or self.JSON_CONTENT_TYPE,
            "Accept":        accept        or self.JSON_CONTENT_TYPE,
        }

    def _url(self, job_id: str | None, is_query: bool) -> str:
        base = self.bulk2_url + ("query" if is_query else "ingest")
        return f"{base}/{job_id}" if job_id else base

    async def create_job(
        self,
        operation: Operation,
        query: str | None = None,
        column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
        line_ending: LineEnding = LineEnding.LF,
        external_id_field: str | None = None,
    ) -> dict[str, Any]:
        is_query = operation in (Operation.query, Operation.query_all)
        payload: dict[str, Any] = {
            "operation":       operation.value,
            "columnDelimiter": column_delimiter.value,
            "lineEnding":      line_ending.value,
        }

        if is_query:
            if not query:
                raise Exception("query string is required for query jobs")
            payload["query"] = query
            headers = self._headers(self.JSON_CONTENT_TYPE, self.CSV_CONTENT_TYPE)
        else:
            payload["object"]      = self.object_name
            payload["contentType"] = "CSV"
            if external_id_field:
                payload["externalIdFieldName"] = external_id_field
            headers = self._headers()

        response = await self._http.request(
            "POST", self._url(None, is_query),
            headers=headers,
            content=json.dumps(payload, allow_nan=False).encode(),
        )
        return response.json()

    async def wait_for_job(
        self,
        job_id: str,
        is_query: bool,
        wait: float = 0.5,
    ) -> None:
        """Exponential backoff poll until JobComplete, Aborted, or Failed."""
        deadline  = datetime.datetime.now() + datetime.timedelta(seconds=self.DEFAULT_WAIT_TIMEOUT_SECONDS)
        delay     = 0.0
        delay_cnt = 0

        await asyncio.sleep(wait)

        while datetime.datetime.now() < deadline:
            info  = await self.get_job(job_id, is_query)
            state = info["state"]

            if state == JobState.job_complete.value:
                return
            if state in (JobState.aborted.value, JobState.failed.value):
                raise Exception(
                    f"Job {job_id} ended with state '{state}': "
                    f"{info.get('errorMessage', info)}"
                )

            if delay < self.MAX_CHECK_INTERVAL_SECONDS:
                delay = wait + math.exp(delay_cnt) / 1000.0
                delay_cnt += 1
            await asyncio.sleep(delay)

        raise Exception(
            f"Job {job_id} timed out after {self.DEFAULT_WAIT_TIMEOUT_SECONDS}s"
        )

    async def get_job(self, job_id: str, is_query: bool) -> dict[str, Any]:
        response = await self._http.request("GET", self._url(job_id, is_query))
        return response.json()

    async def close_job(self, job_id: str) -> dict[str, Any]:
        return await self._set_state(job_id, is_query=False, state=JobState.upload_complete.value)

    async def abort_job(self, job_id: str, is_query: bool) -> dict[str, Any]:
        return await self._set_state(job_id, is_query=is_query, state=JobState.aborted.value)

    async def delete_job(self, job_id: str, is_query: bool) -> dict[str, Any]:
        response = await self._http.request("DELETE", self._url(job_id, is_query))
        return response.json()

    async def _set_state(self, job_id: str, is_query: bool, state: str) -> dict[str, Any]:
        response = await self._http.request(
            "PATCH", self._url(job_id, is_query),
            content=json.dumps({"state": state}, allow_nan=False).encode(),
        )
        return response.json()

    async def upload_job_data(self, job_id: str, data: bytes) -> None:
        """
        PUT CSV bytes to an open ingest job.
        Data is always in-memory bytes from _arrow_to_csv_bytes - never a file path.
        """
        if not data:
            raise Exception("data is required for ingest jobs")
        if len(data) > MAX_INGEST_JOB_FILE_SIZE:
            raise Exception(
                f"Chunk is {len(data)} bytes - exceeds the {MAX_INGEST_JOB_FILE_SIZE} byte "
                "Bulk 2.0 limit. Reduce chunk_size on the upload call."
            )

        response = await self._http.request(
            "PUT",
            self._url(job_id, is_query=False) + "/batches",
            headers=self._headers(self.CSV_CONTENT_TYPE, self.JSON_CONTENT_TYPE),
            content=data,
        )
        if response.status_code != 201:
            raise Exception(f"Upload failed. HTTP {response.status_code}: {response.text}")

    async def get_query_results(
        self,
        job_id: str,
        locator: str = "",
        max_records: int = DEFAULT_QUERY_PAGE_SIZE,
    ) -> QueryBytesResult:
        """
        Fetch one page of query results as raw bytes.
        Caller parses with PyArrow using the cached object schema.
        """
        params: dict[str, Any] = {"maxRecords": max_records}
        if locator and locator != "null":
            params["locator"] = locator

        response = await self._http.request(
            "GET",
            self._url(job_id, is_query=True) + "/results",
            headers=self._headers(self.JSON_CONTENT_TYPE, self.CSV_CONTENT_TYPE),
            params=params,
        )

        next_locator = response.headers.get("Sforce-Locator", "")
        if next_locator == "null":
            next_locator = ""

        return {
            "locator":           next_locator,
            "number_of_records": int(response.headers["Sforce-NumberOfRecords"]),
            "data":              filter_null_bytes(response.content),
        }

    async def get_ingest_results(self, job_id: str, results_type: str) -> bytes:
        """
        Fetch ingest result CSV (successful/failed/unprocessed) as bytes.
        Caller parses with PyArrow using the cached object schema.
        """
        response = await self._http.request(
            "GET",
            self._url(job_id, is_query=False) + f"/{results_type}",
            headers=self._headers(self.JSON_CONTENT_TYPE, self.CSV_CONTENT_TYPE),
        )
        return filter_null_bytes(response.content)