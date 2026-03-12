from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, TYPE_CHECKING
import logging
from .OracleTable import OracleTable
from .OracleUser import OracleUser
from .Csv import Csv
from .Batch import Batch
logger = logging.getLogger(__name__)
if TYPE_CHECKING:
    import argparse

@dataclass
class Job:
    csv: Csv
    oracle_table: OracleTable = field(default_factory=lambda: OracleTable(OracleUser()))
    table: str = ''
    schema: str = ''
    test_run: bool = False
    batch_size: int = 1000
    batch: list[Batch] = field(default_factory=list)
    main_dir: Path = Path('')
    processed_dir: Path = Path('')
    final_path: Optional[Path] = None
    error_dir: Path = Path('')
    quarantined: bool = False
    _total_batches_needed: int = 0

    @property
    def success_check(self) -> bool: return not self.quarantined

    @property
    def total_batches_needed(self) -> int:
        if self._total_batches_needed == 0:
            import math
            self._total_batches_needed = int(math.ceil(self.csv.row_count / self.batch_size)) if self.batch_size else 0
        return self._total_batches_needed
    
    @staticmethod
    def build_job(args: 'argparse.Namespace') -> 'Job':
        import inspect
        valid_params = inspect.signature(Job.__init__).parameters
        parsed_args = {k:v for k,v in vars(args).items() if v and k in valid_params and k != 'csv'}
        csv = Csv(source_path=Path(args.source))
        job = Job(csv=csv, **parsed_args)
        if job.table is None or job.table == '': job.table = OracleTable.to_oracle_snake(job.csv.file_name)
        if job.schema is None or job.schema == '': job.schema = str(job.session().oracle_user).upper()
        return job
    
    def _construct_table(self) -> None:
        from .exceptions import quarantine_file, IngestionError, QuarantineError
        try:
            self.oracle_table.table_name = self.table
            self.oracle_table.schema_name = self.schema
            self.oracle_table.test_run = self.test_run
            self.oracle_table = OracleTable.construct_table(self.csv.file_headers, self.oracle_table)
            if self.oracle_table is None:
                raise ValueError('Error: Oracle Table Never Initialized')
        except (QuarantineError, IngestionError) as e:
            logger.error(f'Error: Job._construct_table failed executing: quarantine_file{e}')
            quarantine_file(str(e), job=self)
            raise

    def session(self) -> OracleUser:
        return self.oracle_table.session
    
    def move_processed_file(self) -> None:
        import shutil
        try:
            if self.processed_dir and self.csv.source_path:
                source = Path(self.csv.source_path.resolve())
                dest_dir = Path(self.processed_dir.resolve())
            else:
                raise ValueError('Path error on source_path or error_dir')
            dest_dir.mkdir(parents=True, exist_ok=True)
            dest = dest_dir / source.name
            if dest.exists():
                stem = source.stem; suffix = source.suffix; counter = 1
                while dest.exists():
                    dest = dest_dir / f'{stem}_{counter}{suffix}'; counter += 1
            shutil.move(str(source), str(dest))
        except OSError as e:
            logger.error(f'Failed to move {source} file to {dest}: {e}')
        except Exception as e:
            logger.error(f'Job.move_processed_file : Critical Error: {e}')
            raise RuntimeError(e)
        
    def _process_batches(self) -> None:
        from .exceptions import IngestionError, quarantine_file
        has_errors = False
        row_stream = self.csv.rows()
        for i in range(self.total_batches_needed):
            batch_obj = Batch.batch_exec(job=self, row_stream=row_stream, size=self.batch_size)
            self.batch.append(batch_obj)
            logger.info(f'Batch {i+1} processed {batch_obj.total_rows} rows.')
            if batch_obj.error_count > 0:
                logger.error(f'Batch {i+1} had {batch_obj.error_count} errors.')
                has_errors = True
        if has_errors:
            quarantine_file(job=self)
            raise IngestionError('batch ingestion failed')

def job_exec(args: argparse.Namespace) -> int:
    logger.info(f'job_exec executing for file: {args.source}')
    try:
        job = Job.build_job(args)
        job._construct_table()
        job._process_batches()
        job.move_processed_file()
        job.session().close_con()
        return 0
    except Exception as e:
        logger.error(f'job_exec Failure Error: {e}')
        raise
