from __future__ import annotations

# standard library
import json
import os
import datetime
import csv
import math
import tempfile
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from functools import partial
from time import sleep
import http.client as http_status
from collections.abc import Generator
from collections import OrderedDict

# Dependencies
from more_itertools import chunked

# locals
from models import (
    Operation, JobState, ColumnDelimiter, LineEnding, ResultsType,
    QueryRecordsResult, QueryFileResult, QueryParameters
)
from utils.csv_utils import (
    ColumnDelimiter, LineEnding, QUOTING_TYPE,
    split_csv, count_csv, convert_dict_to_csv, get_csv_fieldnames,
    MAX_INGEST_JOB_FILE_SIZE, MAX_INGEST_JOB_PARALLELISM, DEFAULT_QUERY_PAGE_SIZE # DELIMITERS, 
)
from utils.date_to_iso8601 import date_to_iso8601
from utils.filter_null_bytes import filter_null_bytes

# Type Checking
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from typing import Any, Literal
    from enum import Enum
    from server.connectors.sf.HttpClient import HttpClient
    

# Logging
import logging
logger = logging.getLogger(__name__)

#####################################################################
#####################################################################
#####################################################################

class SfBulk2Handler:
    """
    Entry point for Bulk API operations.
    Usage: sf.bulk2.Contact.insert(...)
    """
    http_client: HttpClient
    bulk2_url: str
    def __init__(self, http_client: HttpClient):
        self._http = http_client
        self.bulk2_url = f"{self._http.base_url}/jobs/"

    def query(self, soql: str, **kwargs: Any) -> Any:
        return SfBulk2Type(object_name='_query', bulk2_url=self.bulk2_url, http_client=self._http).query(soql, **kwargs)

    def query_all(self, soql: str, **kwargs: Any) -> Any:
        return SfBulk2Type(object_name='_query', bulk2_url=self.bulk2_url, http_client=self._http).query_all(soql, **kwargs)

    def __dir__(self) -> list[str]:
        # Dynamic SObject names
        return list(super().__dir__())

    def __getattr__(self, name: str) -> SfBulk2Type:
        if name.startswith('__'):
            return super().__getattribute__(name)

        return SfBulk2Type(
            object_name=name, 
            bulk2_url=self.bulk2_url, 
            http_client=self._http
            )

######################################################################
######################################################################
######################################################################

class SfBulk2Type:
    """High-level Interface to Bulk 2.0 API functions for a specific SObject."""
    object_name: str
    bulk2_url: str
    _http: HttpClient
    _client: _Bulk2Client

    def __init__(
            self, 
            object_name: str, 
            bulk2_url: str, 
            http_client: HttpClient
            ):
        self.object_name = object_name
        self.bulk2_url = bulk2_url
        self._http = http_client
        self._client = _Bulk2Client(object_name, bulk2_url, http_client)

    def _upload_data(
            self, 
            operation: Operation, 
            data: str | tuple[int, str], 
            column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA, 
            line_ending: LineEnding = LineEnding.LF, 
            external_id_field: str | None = None, 
            wait: int = 5
            ) -> dict[str, int]:
        if isinstance(data, tuple):
            total, unpacked_data = data
        else:
            total = count_csv(data=data, line_ending=LineEnding(line_ending), skip_header=True)
            unpacked_data = data
            
        res = self._client.create_job(operation, column_delimiter=column_delimiter, line_ending=line_ending, external_id_field=external_id_field)
        job_id = res["id"]
        
        try:
            if res["state"] == JobState.open.value:
                self._client.upload_job_data(job_id, unpacked_data)
                self._client.close_job(job_id)
                self._client.wait_for_job(job_id, False, wait)
                res = self._client.get_job(job_id, False)
                return {
                    "numberRecordsFailed": int(res["numberRecordsFailed"]),
                    "numberRecordsProcessed": int(res["numberRecordsProcessed"]),
                    "numberRecordsTotal": int(total),
                    "job_id": job_id,
                }
            raise Exception(f"Failed to upload job data. Response: {res}")
        except Exception:
            res = self._client.get_job(job_id, False)
            if res["state"] in (JobState.upload_complete.value, JobState.in_progress.value, JobState.open.value):
                self._client.abort_job(job_id, False)
            raise

    

    def _upload_file(
            self, 
            operation: Operation, 
            csv_file: str | None = None, 
            records: str | None = None, 
            batch_size: int | None = None, 
            column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA, 
            line_ending: LineEnding = LineEnding.LF, 
            quoting: QUOTING_TYPE = csv.QUOTE_MINIMAL, 
            external_id_field: str | None = None, 
            concurrency: int = 1, 
            wait: int = 5
            ) -> list[dict[str, int]]:
        if csv_file and records:
            raise Exception("Cannot include both file and records")
        elif csv_file and not os.path.exists(csv_file):
            raise Exception(f"{csv_file} not found.")
        elif not csv_file and not records:
            raise Exception("Must include either file or records")

        results = []
        workers = min(concurrency, MAX_INGEST_JOB_PARALLELISM)
        
        split_data = split_csv(filename=csv_file, max_records=batch_size, line_ending=LineEnding(line_ending), column_delimiter=ColumnDelimiter(column_delimiter), quoting=quoting) if csv_file else split_csv(records=records, max_records=batch_size, line_ending=LineEnding(line_ending), column_delimiter=ColumnDelimiter(column_delimiter), quoting=quoting)
        
        if workers == 1:
            for data in split_data:
                results.append(self._upload_data(operation, data, column_delimiter, line_ending, external_id_field, wait))
        else:
            for chunks in chunked(split_data, n=workers):
                workers = min(workers, len(chunks))
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    multi_thread_worker = partial(self._upload_data, operation, column_delimiter=column_delimiter, line_ending=line_ending, external_id_field=external_id_field, wait=wait)
                    _results = pool.map(multi_thread_worker, chunks)
                results.extend(list(_results))
        return results

    def insert(
            self, 
            csv_file: str | None = None, 
            records: list[dict[str, str]] | None = None, 
            batch_size: int | None = None, 
            concurrency: int = 1, 
            column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA, 
            line_ending: LineEnding = LineEnding.LF, 
            quoting: QUOTING_TYPE = csv.QUOTE_MINIMAL, 
            wait: int = 5
            ) -> list[dict[str, int]]:
        return self._upload_file(
            Operation.insert, 
            csv_file=csv_file, 
            records=convert_dict_to_csv(
                records, 
                ColumnDelimiter(column_delimiter), 
                LineEnding(line_ending),
                quoting
                ), 
            batch_size=batch_size,
            column_delimiter=column_delimiter, 
            line_ending=line_ending, 
            quoting=quoting, 
            concurrency=concurrency, 
            wait=wait
            )

    def upsert(self, external_id_field: str, csv_file: str | None = None, records: list[dict[str, str]] | None = None, batch_size: int | None = None, column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA, line_ending: LineEnding = LineEnding.LF, quoting: QUOTING_TYPE = csv.QUOTE_MINIMAL, wait: int = 5) -> list[dict[str, int]]:
        return self._upload_file(Operation.upsert, csv_file=csv_file, records=convert_dict_to_csv(records, ColumnDelimiter(column_delimiter), LineEnding(line_ending), quoting), batch_size=batch_size, column_delimiter=column_delimiter, line_ending=line_ending, quoting=quoting, external_id_field=external_id_field, wait=wait)

    def update(self, csv_file: str | None = None, records: list[dict[str, str]] | None = None, batch_size: int | None = None, column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA, line_ending: LineEnding = LineEnding.LF, quoting: QUOTING_TYPE = csv.QUOTE_MINIMAL, wait: int = 5) -> list[dict[str, int]]:
        return self._upload_file(Operation.update, csv_file=csv_file, records=convert_dict_to_csv(records, ColumnDelimiter(column_delimiter), LineEnding(line_ending), quoting), batch_size=batch_size, column_delimiter=column_delimiter, line_ending=line_ending, quoting=quoting, wait=wait)

    def delete(self, csv_file: str | None = None, records: list[dict[str, str]] | None = None, batch_size: int | None = None, column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA, line_ending: LineEnding = LineEnding.LF, quoting: QUOTING_TYPE = csv.QUOTE_MINIMAL, wait: int = 5) -> list[dict[str, int]]:
        self._constrain_id_only(csv_file, records, column_delimiter, line_ending, quoting)
        return self._upload_file(Operation.delete, csv_file=csv_file, records=convert_dict_to_csv(records, ColumnDelimiter(column_delimiter), LineEnding(line_ending), quoting), batch_size=batch_size, column_delimiter=column_delimiter, line_ending=line_ending, quoting=quoting, wait=wait)

    def _constrain_id_only(self, csv_file: str | None = None, records: list[dict[str, str]] | None = None, column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA, line_ending: LineEnding = LineEnding.LF, quoting: QUOTING_TYPE = csv.QUOTE_MINIMAL) -> None:
        header = get_csv_fieldnames(
            filename=csv_file, 
            records=records, 
            column_delimiter=ColumnDelimiter(column_delimiter), 
            line_ending=LineEnding(line_ending), 
            quoting=quoting
            )
        if str([header][0]).lower() != ["id"]:
            raise Exception(f"InvalidBatch: The 'delete/hard_delete' batch must contain only 'Id', {header}")

    def hard_delete(
            self, 
            csv_file: str | None = None, 
            records: list[dict[str, str]] | None = None, 
            batch_size: int | None = None, 
            column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA, 
            line_ending: LineEnding = LineEnding.LF, 
            quoting: QUOTING_TYPE = csv.QUOTE_MINIMAL, 
            wait: int = 5
            ) -> list[dict[str, int]]:
        
        self._constrain_id_only(
            csv_file, 
            records, 
            column_delimiter, 
            line_ending, 
            quoting
            )
        
        return self._upload_file(
            Operation.hard_delete, 
            csv_file=csv_file, 
            records=convert_dict_to_csv(
                records, 
                ColumnDelimiter(column_delimiter), 
                LineEnding(line_ending), 
                quoting), 
                batch_size=batch_size, column_delimiter=column_delimiter, line_ending=line_ending, 
                quoting=quoting, 
                wait=wait
                )

    def query(self, query: str, max_records: int = DEFAULT_QUERY_PAGE_SIZE, column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA, line_ending: LineEnding = LineEnding.LF, wait: int = 5) -> Generator[str | int , None, None]:
        res = self._client.create_job(Operation.query, query, column_delimiter, line_ending)
        job_id = res["id"]
        self._client.wait_for_job(job_id, True, wait)

        locator = "INIT"
        while locator:
            if locator == "INIT":
                locator = ""
            result = self._client.get_query_results(job_id, locator, max_records)
            locator = result["locator"]
            yield result["records"]

    def query_all(self, query: str, max_records: int = DEFAULT_QUERY_PAGE_SIZE, column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA, line_ending: LineEnding = LineEnding.LF, wait: int = 5) -> Generator[str, None, None]:
        res = self._client.create_job(Operation.query_all, query, column_delimiter, line_ending)
        job_id = res["id"]
        self._client.wait_for_job(job_id, True, wait)

        locator = "INIT"
        while locator:
            if locator == "INIT":
                locator = ""
            result = self._client.get_query_results(job_id, locator, max_records)
            locator = result["locator"]
            yield result["records"]

    def download(
            self, 
            query: str, 
            path: str, 
            max_records: int = DEFAULT_QUERY_PAGE_SIZE,
            column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA, 
            line_ending: LineEnding = LineEnding.LF, 
            wait: int = 5
            ) -> list[QueryFileResult]:
        if not os.path.exists(path):
            raise Exception(f"Path does not exist: {path}")

        res = self._client.create_job(Operation.query, query, column_delimiter, line_ending)
        job_id = res["id"]
        self._client.wait_for_job(job_id, True, wait)

        results = []
        locator = "INIT"
        while locator:
            if locator == "INIT":
                locator = ""
            result = self._client.download_job_data(path, job_id, locator, max_records)
            locator = result["locator"]
            results.append(result)
        return results

    def _retrieve_ingest_records(self, job_id: str, results_type: str, file: str | None = None) -> str:
        if not file:
            return self._client.get_ingest_results(job_id, results_type)
        self._client.download_ingest_results(file, job_id, results_type)
        return ""

    def get_failed_records(self, job_id: str, file: str | None = None) -> str:
        return self._retrieve_ingest_records(job_id, ResultsType.failed.value, file)

    def get_unprocessed_records(self, job_id: str, file: str | None = None) -> str:
        return self._retrieve_ingest_records(job_id, ResultsType.unprocessed.value, file)

    def get_successful_records(self, job_id: str, file: str | None = None) -> str:
        return self._retrieve_ingest_records(job_id, ResultsType.successful.value, file)

    def get_all_ingest_records(self, job_id: str, file: str | None = None) -> dict[str, list[Any]]:
        successful_records = csv.DictReader(self.get_successful_records(job_id=job_id, file=file).splitlines(), delimiter=',', lineterminator='\n')
        failed_records = csv.DictReader(self.get_failed_records(job_id=job_id, file=file).splitlines(), delimiter=',', lineterminator='\n')
        unprocessed_records = csv.DictReader(self.get_unprocessed_records(job_id=job_id, file=file).splitlines(), delimiter=',', lineterminator='\n')
        return {
            'successfulRecords': list(successful_records),
            'failedRecords': list(failed_records),
            'unprocessedRecords': list(unprocessed_records)
        }


######################################################################
######################################################################
######################################################################

class _Bulk2Client:
    """Low-level Bulk 2.0 API HTTP client."""
    JSON_CONTENT_TYPE = "application/json"
    CSV_CONTENT_TYPE = "text/csv; charset=UTF-8"

    DEFAULT_WAIT_TIMEOUT_SECONDS = 7800  # 6 hours
    MAX_CHECK_INTERVAL_SECONDS = 60.0

    def __init__(self, object_name: str, bulk2_url: str, http_client: HttpClient):
        self.object_name = object_name
        self.bulk2_url = bulk2_url
        self._http = http_client

    def _get_headers(self, request_content_type: str | None = None, accept_content_type: str | None = None) -> dict[str, str]:
        return {
            "Content-Type": request_content_type or self.JSON_CONTENT_TYPE,
            "Accept": accept_content_type or self.JSON_CONTENT_TYPE,
        }

    def _construct_request_url(
            self, 
            job_id: str | None, 
            is_query: bool
            ) -> str:
        job_id = job_id or ""
        url = self.bulk2_url + ("query" if is_query else "ingest")
        return f"{url}/{job_id}" if job_id else url

    def create_job(
            self, 
            operation: Operation, 
            query: str | None = None, 
            column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA, 
            line_ending: LineEnding = LineEnding.LF, 
            external_id_field: str | None = None
        ) -> Any:
        payload: dict[str, Any] = {
            "operation": operation.value,
            "columnDelimiter": column_delimiter.value,
            "lineEnding": line_ending.value,
        }
        if external_id_field:
            payload["externalIdFieldName"] = external_id_field

        is_query = operation in (Operation.query, Operation.query_all)
        url = self._construct_request_url(None, is_query)
        
        if is_query:
            headers = self._get_headers(self.JSON_CONTENT_TYPE, self.CSV_CONTENT_TYPE)
            if not query:
                raise Exception("Query is required for query jobs")
            payload["query"] = query
        else:
            headers = self._get_headers(self.JSON_CONTENT_TYPE, self.JSON_CONTENT_TYPE)
            payload["object"] = self.object_name
            payload["contentType"] = "CSV"
            
        response = self._http.request("POST", url, headers=headers, data=json.dumps(payload, allow_nan=False))
        return response.json(object_pairs_hook=OrderedDict)

    def wait_for_job(
            self, 
            job_id: str, 
            is_query: bool, 
            wait: float = 0.5
        ) -> Literal[JobState.job_complete]:
        expiration_time = datetime.datetime.now() + datetime.timedelta(seconds=self.DEFAULT_WAIT_TIMEOUT_SECONDS)
        job_status = JobState.in_progress if is_query else JobState.open
        delay_timeout = 0.0
        delay_cnt = 0
        sleep(wait)
        
        while datetime.datetime.now() < expiration_time:
            job_info = self.get_job(job_id, is_query)
            job_status = job_info["state"]
            if job_status in [JobState.job_complete, JobState.aborted, JobState.failed]:
                if job_status != JobState.job_complete:
                    error_message = job_info.get("errorMessage") or job_info
                    raise Exception(f"Job failure. Response content: {error_message}")
                return job_status  # JobComplete

            if delay_timeout < self.MAX_CHECK_INTERVAL_SECONDS:
                delay_timeout = wait + math.exp(delay_cnt) / 1000.0
                delay_cnt += 1
            sleep(delay_timeout)
            
        raise Exception(f"Job timeout. Job status: {job_status}")

    def abort_job(self, job_id: str, is_query: bool) -> Any:
        return self._set_job_state(job_id, is_query, JobState.aborted.value)

    def close_job(self, job_id: str) -> Any:
        return self._set_job_state(job_id, False, JobState.upload_complete.value)

    def delete_job(self, job_id: str, is_query: bool) -> Any:
        url = self._construct_request_url(job_id, is_query)
        response = self._http.request("DELETE", url, headers=self._get_headers())
        return response.json(object_pairs_hook=OrderedDict)

    def _set_job_state(self, job_id: str, is_query: bool, state: str) -> Any:
        url = self._construct_request_url(job_id, is_query)
        payload = {"state": state}
        response = self._http.request("PATCH", url, headers=self._get_headers(), data=json.dumps(payload, allow_nan=False))
        return response.json(object_pairs_hook=OrderedDict)

    def get_job(self, job_id: str, is_query: bool) -> Any:
        url = self._construct_request_url(job_id, is_query)
        response = self._http.request("GET", url)
        return response.json(object_pairs_hook=OrderedDict)

    def get_query_results(self, job_id: str, locator: str = "", max_records: int = DEFAULT_QUERY_PAGE_SIZE) -> QueryRecordsResult:
        url = self._construct_request_url(job_id, True) + "/results"
        params: QueryParameters = {"maxRecords": max_records}
        if locator and locator != "null":
            params["locator"] = locator
            
        headers = self._get_headers(self.JSON_CONTENT_TYPE, self.CSV_CONTENT_TYPE)
        response = self._http.request("GET", url, headers=headers, params=params)
        
        locator = response.headers.get("Sforce-Locator", "")
        if locator == "null":
            locator = ""
            
        number_of_records = int(response.headers["Sforce-NumberOfRecords"])
        return {
            "locator": locator,
            "number_of_records": number_of_records,
            "records": filter_null_bytes(response.content.decode('utf-8')),
        }

    def download_job_data(
            self, 
            path: str, 
            job_id: str, 
            locator: str = "",
            max_records: int = DEFAULT_QUERY_PAGE_SIZE, 
            chunk_size: int = 1024
            ) -> QueryFileResult:
        if not os.path.exists(path):
            raise Exception(f"Path does not exist: {path}")

        url = self._construct_request_url(job_id, True) + "/results"
        params: QueryParameters = {"maxRecords": max_records}
        if locator and locator != "null":
            params["locator"] = locator
            
        headers = self._get_headers(self.JSON_CONTENT_TYPE, self.CSV_CONTENT_TYPE)
        
        with closing(self._http.request("GET", url, headers=headers, params=params, stream=True)) as response, \
             tempfile.NamedTemporaryFile("wb", dir=path, suffix=".csv", delete=False) as bos:
            
            locator = response.headers.get("Sforce-Locator", "")
            if locator == "null":
                locator = ""
            number_of_records = int(response.headers["Sforce-NumberOfRecords"])
            
            for chunk in response.iter_content(chunk_size=chunk_size):
                bos.write(filter_null_bytes(chunk))
                
            if os.path.isfile(bos.name):
                return {
                    "locator": locator,
                    "number_of_records": number_of_records,
                    "file": bos.name,
                }
            raise Exception(f"IO/Error occurred verifying binary data. File {bos.name} doesn't exist.")

    def upload_job_data(self, job_id: str, data: str, content_url: str | None = None) -> None:
        if not data:
            raise Exception("Data is required for ingest jobs")

        data_size = len(data.encode("utf-8"))
        if data_size > MAX_INGEST_JOB_FILE_SIZE:
            raise Exception(f"Data size {data_size} exceeds Bulk V2 max (100 MB)")

        url = content_url or (self._construct_request_url(job_id, False) + "/batches")
        headers = self._get_headers(self.CSV_CONTENT_TYPE, self.JSON_CONTENT_TYPE)
        response = self._http.request("PUT", url, headers=headers, data=data.encode("utf-8"))
        
        if response.status_code != http_status.CREATED:
            raise Exception(f"Upload failed. HTTP {response.status_code}: {response.content.decode()}")

    def get_ingest_results(self, job_id: str, results_type: str) -> str:
        url = self._construct_request_url(job_id, False) + "/" + results_type
        headers = self._get_headers(self.JSON_CONTENT_TYPE, self.CSV_CONTENT_TYPE)
        response = self._http.request("GET", url, headers=headers)
        return response.text

    def download_ingest_results(self, file: str, job_id: str, results_type: str, chunk_size: int = 1024) -> None:
        url = self._construct_request_url(job_id, False) + "/" + results_type
        headers = self._get_headers(self.JSON_CONTENT_TYPE, self.CSV_CONTENT_TYPE)
        
        with closing(self._http.request("GET", url, headers=headers, stream=True)) as response, \
             open(file, "wb") as bos:
            for chunk in response.iter_content(chunk_size=chunk_size):
                bos.write(filter_null_bytes(chunk))

        if not os.path.exists(file):
            raise Exception(f"IO/Error verifying binary data. File {file} doesn't exist.")



