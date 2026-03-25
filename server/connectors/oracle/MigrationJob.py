"""python ../../master/master.py --config ../../.secrets/.env -l ./logs -v --env homelab --exec python ./MigrationJob.py"""
import logging #, os, sys, io
from pathlib import Path
from cryptography.fernet import Fernet
from oracle.OracleJob import Job

logger = logging.getLogger(__name__)

class MigrationJob:
    _key: bytes | None
    object: str
    source_path: str
    def __init__(self, 
                 source_path: str, 
                 object_name: str):
        self.source_path = source_path
        self.object_name = object_name
        self._key = None

    @property
    def key(self) -> bytes:
        if self._key is None: self._key = Fernet.generate_key()
        return self._key

    def encrypt_file(self) -> None:
        file = Path(self.source_path)
        file.write_bytes(Fernet(self.key).encrypt(file.read_bytes()))

    def execute_oracle(self) -> int:
        logger.debug("Running Oracle main function")
        job=Job(
            source_path=self.source_path,
            table=self.object,
            key=self.key,
        )
        result: int = job.run_job()
        return result
    
    def run(self) -> int:
        self.encrypt_file()
        result = self.execute_oracle()
        return result

def main():
    for obj in ["Account", "Contact", "Lead", "Case", "User"]:
        file=f"Q:/lab/prod/test/data/{obj}.csv"
        
        mijob = MigrationJob(source_path=file, object_name=obj)
        result = mijob.run()
        if result != 0:
            logger.error(f"Migration for {obj} completed with errors. Stopping further migrations.")
            raise RuntimeError(f"Migration for {obj} failed with result code: {result}")

    logger.info("--------------All migrations completed--------------")
if __name__ == "__main__":
    main()