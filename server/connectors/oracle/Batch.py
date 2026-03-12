from __future__ import annotations
import itertools, logging
from dataclasses import dataclass
from typing import Iterator
logger = logging.getLogger(__name__)

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .Job import Job

@dataclass
class Batch:
    rows_processed_count: int = 0
    total_rows: int = 0
    error_count: int = 0
    message: str = ''
    all_rows_failed: bool = False
    @staticmethod
    def batch_exec(job: Job, row_stream: Iterator[list[str]], size: int) -> Batch:
        from .normalize_data import normalize_cell
        batch = Batch()
        raw_rows = list(itertools.islice(row_stream, size))
        if not raw_rows: return batch
        batch.total_rows = len(raw_rows)
        active_plan = [(col.csv_index, col.bind_name, col.data_type) for col in job.oracle_table.column_map.values() if col.csv_index is not None]
        formatted_data = [
            {bind_name: normalize_cell(row[idx], dtype) for idx, bind_name, dtype in active_plan}
            for row in raw_rows
        ]
        batch.rows_processed_count = len(formatted_data)
        batch.error_count = batch.batch_execute_inserts(sql=job.oracle_table.insert_sql_stmt, row_list=formatted_data, input_sizes=job.oracle_table.build_input_sizes(), connection=job.oracle_table.session.get_con(), test_run=job.test_run)
        if batch.error_count > 0:
            batch.all_rows_failed = batch.error_count == batch.total_rows
            batch.message = f"""------ Batch Has Errors ------
                                total_rows={batch.total_rows}
                                error_count={batch.error_count}
                                all_rows_failed={batch.all_rows_failed}
                                """
            logger.error(batch.message)
        else:
            batch.message = 'Batch Success'; logger.info(batch.message)
        return batch
    def batch_execute_inserts(self, sql, row_list, connection, input_sizes=None, batcherrors=True, test_run=False) -> int:
        try:
            cursor = connection.cursor(); total_errors = 0; iterator = iter(row_list); batch_size = 10000
            while True:
                chunk = list(itertools.islice(iterator, batch_size))
                if not chunk: break
                with cursor:
                    if input_sizes: cursor.setinputsizes(**input_sizes)
                    cursor.executemany(sql, chunk, batcherrors=batcherrors)
                    batch_errors = cursor.getbatcherrors()
                    if batch_errors:
                        total_errors += len(batch_errors)
                        logger.error(f'Batch Errors in chunk: {batch_errors}')
                        connection.rollback()
                        return total_errors
            if not test_run: connection.commit()
            else: connection.rollback()
            return total_errors
        except Exception:
            connection.rollback(); raise
