from __future__ import annotations
import io
import logging
from dataclasses import dataclass
from pathlib import Path
import pandas as pd
from cryptography.fernet import Fernet
from .OracleModels import OracleTable, to_oracle_snake, normalize_cell
from .OracleClient import OracleClient
logger = logging.getLogger(__name__)

class Job:
    oracle_table: OracleTable
    oracle_client: OracleClient
    table: str
    schema: str
    batch_size: int
    batch: list[Batch]
    col_dict: dict[str, dict[str, str]]
    _key: bytes

    def __init__(self,
                 source_path: Path|str,
                 key: bytes,
                 oracle_client: OracleClient = OracleClient(),
                 table: str = '',
                 schema: str = '',
                 batch_size: int = 10000,
                 ):
        self.col_dict = {}
        self.oracle_client = oracle_client
        self.schema = schema.upper() or str(oracle_client.oracle_user).upper() or ''
        self.source_path = Path(source_path).expanduser().resolve() 
        if not self.source_path.is_file(): raise FileNotFoundError(f'Source file not found: {self.source_path}')
        self.file_name = self.source_path.name
        self.table = to_oracle_snake(table.upper() or self.file_name.rsplit('.', 1)[0].upper(), reserved_prefix='SF')
        self.batch_size = batch_size
        self._key = key
        buffer: io.BytesIO = self.get_bytes()
        df_head = pd.read_csv(buffer, nrows=0, encoding='utf-8')
        file_headers = df_head.columns.tolist()
        if not file_headers: raise ValueError('CSV file has no headers.')
        self.col_count = len(file_headers)
        
        for i, h in enumerate(file_headers):
            if not h.strip(): raise ValueError(f'Header at position {i} is blank.')
            base_name = to_oracle_snake(h)
            target_name = base_name
            counter=2
            while target_name in self.col_dict:
                target_name = f'{base_name}_{counter}'
                counter += 1
            self.col_dict[target_name] = {
                                          "target_name": target_name, 
                                          "csv_col_name": h, 
                                          "index": str(i)
                                          }
        self.validate_row_alignment()
        self.batch = []
    
    def validate_row_alignment(self) -> None:
        max_lengths = [0] * self.col_count
        row_count = 0
        buffer: io.BytesIO = self.get_bytes()
        for chunk in pd.read_csv(buffer,
                                dtype=str,
                                keep_default_na=False,
                                encoding='utf-8',
                                on_bad_lines='error',
                                chunksize=10000):
            if len(chunk.columns) != self.col_count:
                raise ValueError(f'Column count mismatch: expected {self.col_count}, got {len(chunk.columns)}')
            chunk_max = chunk.apply(lambda s: s.str.len().max()).fillna(0).astype(int).tolist()
            max_lengths = [max(a, b) for a, b in zip(max_lengths, chunk_max)]
            row_count += len(chunk)
        for col in self.col_dict.values():
            col['max_col_size'] = str(max_lengths[int(col['index'])])
        self.row_count = row_count
        self.alignment_validated = True
        max_row_bytes = sum(max_lengths)
        self.effective_batch_size = max(1, min(self.batch_size, (50 * 1024 * 1024) // max_row_bytes)) if max_row_bytes else self.batch_size

    def get_bytes(self) -> io.BytesIO:
        file_path = Path(self.source_path)
        plaintext = Fernet(self._key).decrypt(file_path.read_bytes())
        return io.BytesIO(plaintext)
    
    def run_job(self) -> int:
        self.oracle_table = OracleTable.construct_table(self.col_dict, self.table, self.schema, self.oracle_client)
        if self.effective_batch_size != self.batch_size:
            logger.info(f'Batch size reduced from {self.batch_size} to {self.effective_batch_size} rows based on max row size')
        batch_start = 2  # row 1 is the header
        batch_count = 0
        con = self.oracle_client.get_con()
        try:
            buffer: io.BytesIO = self.get_bytes()
            for chunk in pd.read_csv(buffer,
                                     dtype=str,
                                     keep_default_na=False,
                                     encoding='utf-8',
                                     on_bad_lines='error',
                                     chunksize=self.effective_batch_size):
                
                batch = Batch.batch_exec(self, chunk, batch_start)
                batch_count += 1
                self.batch.append(batch)
                batch_start += batch.total_rows
                logger.debug(f'Completed batch {batch_count} | rows {batch_start - batch.total_rows}-{batch_start - 1} | 'f'processed {batch.rows_processed_count} rows with {batch.error_count} errors.')
                if batch.failed:
                    logger.error("Job aborted due to row errors. con.rollback()....")
                    con.rollback()
                    return 1
                con.commit()

            logger.info("Job completed successfully. All batches committed.")
            return 0
            
        except pd.errors.ParserError as e:
            logger.error(f'Critical CSV Parsing Error (Likely misaligned row or unclosed quote): {e}')
            con.rollback()
            return 1
        except Exception as e:
            logger.error(f'Unexpected error during job execution: {e}')
            con.rollback()
            return 1

@dataclass
class Batch:
    rows_processed_count: int = 0
    total_rows: int = 0
    error_count: int = 0
    message: str = ''
    failed: bool = False
    
    @staticmethod
    def batch_exec(job: Job, chunk: pd.DataFrame, batch_start: int = 2) -> Batch:
        batch = Batch()
        if chunk.empty: return batch
        batch.total_rows = len(chunk)
        active_plan = job.oracle_table.active_plan

        formatted_data = [
            {bind_name: normalize_cell(row[idx], dtype) for idx, bind_name, dtype in active_plan}
            for row in chunk.itertuples(index=False, name=None)
        ]

        batch.rows_processed_count = len(formatted_data)
        connection = job.oracle_client.get_con()
        try:
            with connection.cursor() as cursor:
                input_sizes = job.oracle_table.build_input_sizes()
                if input_sizes: cursor.setinputsizes(**input_sizes)
                cursor.executemany(job.oracle_table.insert_sql_stmt, formatted_data, batcherrors=True)
                batch_errors = cursor.getbatcherrors()
        except Exception:
            connection.rollback(); raise

        if batch_errors:
            batch_end = batch_start + len(formatted_data) - 1
            failed_lines = '\n'.join(f'  Row {batch_start + e.offset}: {e.message.strip()}' for e in batch_errors)
            logger.error(
                f'Batch failed | rows {batch_start}-{batch_end} | {len(batch_errors)} error(s):\n{failed_lines}\n'
                f'  Example: Row {batch_start + batch_errors[0].offset}: {batch_errors[0].message.strip()}'
            )
            batch.error_count = len(batch_errors)
            batch.failed = True
            batch.message = f"""------ Batch Has Errors ------
                                total_rows={batch.total_rows}
                                error_count={batch.error_count}
                                file_aborted=True
                                """
            logger.error(batch.message)
        else:
            batch.message = 'Batch Success'; logger.info(batch.message)
        return batch