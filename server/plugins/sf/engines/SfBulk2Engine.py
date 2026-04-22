"""
https://developer.salesforce.com/docs/apis
https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_concepts.htm
https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/access_for_fields.htm
https://developer.salesforce.com/docs/atlas.en-us.soql_sosl.meta/soql_sosl/sforce_api_calls_soql_sosl_intro.htm
https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/bulk_api_2_0.htm

"""
from __future__ import annotations

import io
import json
import time
import datetime
import math
from collections.abc import Iterator
from typing import Any, TYPE_CHECKING, TypedDict

import pyarrow as pa
import pyarrow.csv as pa_csv

from server.plugins.sf.models.SfModels import Operation, JobState, ColumnDelimiter, LineEnding, ResultsType

if TYPE_CHECKING:
    from server.plugins.sf.engines.SfClient import SfClient

import logging
logger = logging.getLogger(__name__)

MAX_INGEST_JOB_FILE_SIZE = 150 * 1024 * 1024   # 150 MB per job
MAX_INGEST_JOB_PARALLELISM = 15
DEFAULT_QUERY_PAGE_SIZE = 50_000

class QueryBytesResult(TypedDict):
    locator: str
    number_of_records: int
    data: bytes

########################################################
# Bulk2
########################################################
class Bulk2:
    """
    Entry point for Bulk 2.0 operations.
    Usage: 
        sf = Salesforce(...)
        bulk2 = sf.Bulk2
        sf.bulk2.Contact.insert(table)
        sf.bulk2.query("SELECT Id FROM Contact")
    """
    _http: SfClient
    bulk2_url: str
    headers: dict[str, str]

    def __init__(self, http_client: SfClient) -> None:
        self._http = http_client
        self.bulk2_url = f"{self._http.services_url}/jobs/"
        # self.headers = {
        #     "Content-Type": "application/json",
        #     }
        # "Authorization": "Bearer " + self._http.access_token,
        # "X-PrettyPrint": "1",

    def __getattr__(self, name: str) -> Bulk2SObject:
        if name.startswith("__"):
            return super().__getattribute__(name)
        return Bulk2SObject(object_name=name, bulk2_url=self.bulk2_url, http_client=self._http)

    def query(self, soql: str, **kwargs: Any) -> Iterator[bytes]:
        yield from Bulk2SObject("_query", self.bulk2_url, self._http).query(soql, **kwargs)

    def query_all(self, soql: str, **kwargs: Any) -> Iterator[bytes]:
        yield from Bulk2SObject("_query", self.bulk2_url, self._http).query_all(soql, **kwargs)


########################################################
# Bulk2SObject
########################################################

class Bulk2SObject:
    """High-level Bulk 2.0 interface for a specific SObject.
    Each SObject (e.g. Contact) is represented as a Bulk2SObject, which provides insert/upsert/update/delete methods for ingest and query/query_all for bulk queries. The object_name "_query" is reserved for bulk query operations that don't have a specific SObject context.
    """
    object_name: str
    bulk2_url: str
    _http: SfClient
    _client: _Bulk2Client

    def __init__(self, object_name: str, bulk2_url: str, http_client: SfClient) -> None:
        self.object_name = object_name
        self.bulk2_url = bulk2_url
        self._http = http_client
        self._client = _Bulk2Client(object_name, bulk2_url, http_client)


    

    ########################################################
    # Arrow / CSV utilities
    ########################################################
    @staticmethod
    def _csv_bytes_to_arrow_from_schema(data: bytes, schema: pa.Schema) -> pa.Table:
        """Parse raw CSV bytes into an Arrow table using an explicit schema."""
        if not data.strip(): return pa.table({}, schema=schema)

        # Salesforce Bulk V2 CSV emits time fields as 'HH:MM:SS.sssZ'.
        # PyArrow's CSV reader rejects the trailing Z for time64 columns.
        # Parse those fields as strings first, then strip Z and cast afterward.
        time64_cols = {
            schema.field(i).name: schema.field(i).type
            for i in range(len(schema))
            if pa.types.is_time(schema.field(i).type)
        }
        csv_types = {
            schema.field(i).name: (pa.string() if schema.field(i).name in time64_cols else schema.field(i).type)
            for i in range(len(schema))
        }

        table = pa_csv.read_csv(
            io.BytesIO(data),
            convert_options=pa_csv.ConvertOptions(column_types=csv_types),
        )

        if not time64_cols:
            return table

        cols: dict[str, pa.Array] = {}
        for name in table.schema.names:
            col = table.column(name)
            if name in time64_cols:
                py_times = [
                    None if v is None else datetime.time.fromisoformat(v.rstrip('Z'))
                    for v in col.to_pylist()
                ]
                col = pa.array(py_times, type=time64_cols[name])
            cols[name] = col
        return pa.table(cols, schema=schema)
    @staticmethod
    def csv_bytes_to_arrow(data: bytes, schema: pa.Schema | None = None) -> pa.Table:
        """Parse raw CSV bytes with type inference, used for unknown result shapes."""
        if not data.strip(): return pa.table({})
        if schema is not None:
            return Bulk2SObject._csv_bytes_to_arrow_from_schema(data, schema)
        return pa_csv.read_csv(io.BytesIO(data))
    
    @staticmethod
    def _arrow_to_csv_bytes(table: pa.Table, line_ending: LineEnding = LineEnding.LF, include_header: bool = True) -> bytes:
        buf = io.BytesIO()
        pa_csv.write_csv(table, buf, write_options=pa_csv.WriteOptions(include_header=include_header))
        data = buf.getvalue()
        if line_ending == LineEnding.CRLF: data = data.replace(b"\n", b"\r\n")
        else: data = data.replace(b"\r\n", b"\n")
        return data

    ########################################################
    # --- Ingest internals ---
    ########################################################

    @staticmethod
    def _split_table(table: pa.Table, chunk_size: int | None) -> list[pa.Table]:
        size = chunk_size or (MAX_INGEST_JOB_FILE_SIZE // 500)
        return [table.slice(i, size) for i in range(0, len(table), size)]
    
    def _upload_chunk(
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
        res = self._client.create_job(
            operation,
            column_delimiter=column_delimiter,
            line_ending=line_ending,
            external_id_field=external_id_field,
        )
        job_id = res.get("id", None) or res.get("jobId", None) or res.get("job_id", None)  # API inconsistency between query and ingest job creation
        if not job_id: raise Exception(f"Failed to create job for chunk upload: {res}")

        try:
            if res.get("state", "") != JobState.open.value:
                raise Exception(f"Job {job_id} created in unexpected state: {res.get('state', '')}")

            self._client.upload_job_data(job_id, data)
            self._client.close_job(job_id)
            self._client.wait_for_job(job_id, is_query=False, wait=wait)
            res = self._client.get_job(job_id, is_query=False)

            return {
                "numberRecordsFailed":    int(res.get("numberRecordsFailed", 0)),
                "numberRecordsProcessed": int(res.get("numberRecordsProcessed", 0)),
                "numberRecordsTotal":     record_count,
                "job_id":                 job_id,
            }

        except Exception:
            try:
                current = self._client.get_job(job_id, is_query=False)
                if current.get("state", "") in (
                    JobState.upload_complete.value,
                    JobState.in_progress.value,
                    JobState.open.value,
                ):
                    self._client.abort_job(job_id, is_query=False)
            except Exception as abort_err:
                logger.warning(f"Failed to abort job {job_id} during cleanup: {abort_err}")
            raise

    def _upload_table(
        self,
        operation: Operation,
        table: pa.Table,
        chunk_size: int | None = None,
        column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
        line_ending: LineEnding = LineEnding.LF,
        external_id_field: str | None = None,
        wait: int = 5,
    ) -> list[dict[str, int]]:
        """Serialize each Arrow table chunk to CSV bytes and upload as Bulk 2.0 jobs."""
        chunks = self._split_table(table, chunk_size)
        results: list[dict[str, int]] = []
        for chunk in chunks:
            result = self._upload_chunk(
                operation, Bulk2SObject._arrow_to_csv_bytes(chunk, line_ending), len(chunk),
                column_delimiter, line_ending, external_id_field, wait,
            )
            results.append(result)
        return results

    ########################################################
    # --- Ingest operations ---
    ########################################################
    def insert(
        self,
        table: pa.Table,
        chunk_size: int | None = None,
        line_ending: LineEnding = LineEnding.LF,
        wait: int = 5,
    ) -> list[dict[str, int]]:
        return self._upload_table(Operation.insert, table, chunk_size=chunk_size, line_ending=line_ending, wait=wait)

    def upsert(
        self,
        table: pa.Table,
        external_id_field: str,
        chunk_size: int | None = None,
        line_ending: LineEnding = LineEnding.LF,
        wait: int = 5,
    ) -> list[dict[str, int]]:
        return self._upload_table(
            Operation.upsert, table,
            chunk_size=chunk_size, line_ending=line_ending,
            external_id_field=external_id_field, wait=wait,
        )

    def update(
        self,
        table: pa.Table,
        chunk_size: int | None = None,
        line_ending: LineEnding = LineEnding.LF,
        wait: int = 5,
    ) -> list[dict[str, int]]:
        return self._upload_table(Operation.update, table, chunk_size=chunk_size, line_ending=line_ending, wait=wait)

    def delete(
        self,
        table: pa.Table,
        line_ending: LineEnding = LineEnding.LF,
        wait: int = 5,
    ) -> list[dict[str, int]]:
        self._constrain_id_only(table)
        return self._upload_table(Operation.delete, table, line_ending=line_ending, wait=wait)

    def hard_delete(
        self,
        table: pa.Table,
        line_ending: LineEnding = LineEnding.LF,
        wait: int = 5,
    ) -> list[dict[str, int]]:
        self._constrain_id_only(table)
        return self._upload_table(Operation.hard_delete, table, line_ending=line_ending, wait=wait)

    @staticmethod
    def _constrain_id_only(table: pa.Table) -> None:
        if [c.lower() for c in table.column_names] != ["id"]:
            raise Exception(
                f"Delete operations require a table with only an 'Id' column. "
                f"Got: {table.column_names}"
            )
        
    ########################################################
    # --- Bulk2 Query operations ---
    ########################################################

    def query(
        self,
        query: str,
        max_records: int = DEFAULT_QUERY_PAGE_SIZE,
        line_ending: LineEnding = LineEnding.LF,
        wait: int = 5,
    ) -> Iterator[bytes]:
        """Generator - yields raw CSV bytes per page."""
        res = self._client.create_job(
            Operation.query, query=query, line_ending=line_ending
        )
        job_id = res["id"]
        self._client.wait_for_job(job_id, is_query=True, wait=wait)

        locator = ""
        first = True
        while first or locator:
            first = False
            result = self._client.get_query_results(job_id, locator, max_records)
            locator = result["locator"]
            yield result["data"]

    def query_all(
        self,
        query: str,
        max_records: int = DEFAULT_QUERY_PAGE_SIZE,
        line_ending: LineEnding = LineEnding.LF,
        wait: int = 5,
    ) -> Iterator[bytes]:
        """Includes soft-deleted records. Same yield contract as query()."""
        res = self._client.create_job(
            Operation.query_all, query=query, line_ending=line_ending
        )
        job_id = res["id"]
        self._client.wait_for_job(job_id, is_query=True, wait=wait)

        locator = ""
        first = True
        while first or locator:
            first = False
            result = self._client.get_query_results(job_id, locator, max_records)
            locator = result["locator"]
            yield result["data"]

    # --- Ingest result retrieval ---

    def get_successful_records(self, job_id: str) -> bytes:
        return self._client.get_ingest_results(job_id, ResultsType.successful.value)

    def get_failed_records(self, job_id: str) -> bytes:
        return self._client.get_ingest_results(job_id, ResultsType.failed.value)

    def get_unprocessed_records(self, job_id: str) -> bytes:
        return self._client.get_ingest_results(job_id, ResultsType.unprocessed.value)

    def get_all_ingest_results(self, job_id: str) -> dict[str, bytes]:
        return {
            "successfulRecords":  self.get_successful_records(job_id),
            "failedRecords":      self.get_failed_records(job_id),
            "unprocessedRecords": self.get_unprocessed_records(job_id),
        }


########################################################
# _Bulk2Client
########################################################
class _Bulk2Client:
    """Low-level Bulk 2.0 HTTP operations. Not for direct use outside Bulk2SObject."""
    JSON_CONTENT_TYPE = "application/json"
    CSV_CONTENT_TYPE = "text/csv; charset=UTF-8"
    DEFAULT_WAIT_TIMEOUT_SECONDS = 7800
    MAX_CHECK_INTERVAL_SECONDS = 60.0

    def __init__(self, object_name: str, bulk2_url: str, http_client: SfClient) -> None:
        self.object_name = object_name
        self.bulk2_url = bulk2_url
        self._http = http_client

    def _headers(self, content_type: str | None = None, accept: str | None = None) -> dict[str, str]:
        return {
            "Content-Type": content_type or self.JSON_CONTENT_TYPE,
            "Accept": accept or self.JSON_CONTENT_TYPE,
        }

    def _url(self, job_id: str | None, is_query: bool) -> str:
        base = self.bulk2_url + ("query" if is_query else "ingest")
        return f"{base}/{job_id}" if job_id else base

    def create_job(
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
            if not query: raise Exception("query string is required for query jobs")
            payload["query"] = query
            headers = self._headers(self.JSON_CONTENT_TYPE, self.CSV_CONTENT_TYPE)
        else:
            payload["object"]      = self.object_name
            payload["contentType"] = "CSV"
            if external_id_field:
                payload["externalIdFieldName"] = external_id_field
            headers = self._headers()

        response = self._http.request(
            "POST", self._url(None, is_query),
            headers=headers,
            content=json.dumps(payload, allow_nan=False).encode(),
        )
        return response.json()

    def wait_for_job(
        self,
        job_id: str,
        is_query: bool,
        wait: float = 0.5,
    ) -> None:
        """Exponential backoff poll until JobComplete, Aborted, or Failed."""
        deadline  = datetime.datetime.now() + datetime.timedelta(seconds=self.DEFAULT_WAIT_TIMEOUT_SECONDS)
        delay     = 0.0
        delay_cnt = 0

        time.sleep(wait)

        while datetime.datetime.now() < deadline:
            info  = self.get_job(job_id, is_query)
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
            time.sleep(delay)

        raise Exception(f"Job {job_id} timed out after {self.DEFAULT_WAIT_TIMEOUT_SECONDS}s")

    def get_job(self, job_id: str, is_query: bool) -> dict[str, Any]:
        response = self._http.request("GET", self._url(job_id, is_query))
        return response.json()

    def close_job(self, job_id: str) -> dict[str, Any]:
        return self._set_state(job_id, is_query=False, state=JobState.upload_complete.value)

    def abort_job(self, job_id: str, is_query: bool) -> dict[str, Any]:
        return self._set_state(job_id, is_query=is_query, state=JobState.aborted.value)

    def delete_job(self, job_id: str, is_query: bool) -> dict[str, Any]:
        response = self._http.request("DELETE", self._url(job_id, is_query))
        return response.json()

    def _set_state(self, job_id: str, is_query: bool, state: str) -> dict[str, Any]:
        response = self._http.request(
            "PATCH", self._url(job_id, is_query),
            content=json.dumps({"state": state}, allow_nan=False).encode(),
        )
        return response.json()

    def upload_job_data(self, job_id: str, data: bytes) -> None:
        if not data: raise Exception("data is required for ingest jobs")
        if len(data) > MAX_INGEST_JOB_FILE_SIZE:
            raise Exception(
                f"Chunk is {len(data)} bytes - exceeds the {MAX_INGEST_JOB_FILE_SIZE} byte "
                "Bulk 2.0 limit. Reduce chunk_size on the upload call."
            )

        response = self._http.request(
            "PUT",
            self._url(job_id, is_query=False) + "/batches",
            headers=self._headers(self.CSV_CONTENT_TYPE, self.JSON_CONTENT_TYPE),
            content=data,
        )
        if response.status_code != 201:
            raise Exception(f"Upload failed. HTTP {response.status_code}: {response.text}")

    def get_query_results(
        self,
        job_id: str,
        locator: str = "",
        max_records: int = DEFAULT_QUERY_PAGE_SIZE,
    ) -> QueryBytesResult:
        params: dict[str, Any] = {"maxRecords": max_records}
        if locator and locator != "null":
            params["locator"] = locator

        response = self._http.request(
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

    def get_ingest_results(self, job_id: str, results_type: str) -> bytes:
        response = self._http.request(
            "GET",
            self._url(job_id, is_query=False) + f"/{results_type}",
            headers=self._headers(self.JSON_CONTENT_TYPE, self.CSV_CONTENT_TYPE),
        )
        return filter_null_bytes(response.content)



from typing import AnyStr

def filter_null_bytes(b: AnyStr) -> AnyStr:
        """https://github.com/airbytehq/airbyte/issues/8300"""
        if isinstance(b, str):
            return b.replace("\x00", "")
        if isinstance(b, bytes):
            return b.replace(b"\x00", b"")
        raise TypeError("Expected str or bytes")