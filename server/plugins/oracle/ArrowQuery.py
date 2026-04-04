from __future__ import annotations
import io, os, tempfile, logging
import oracledb
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from cryptography.hazmat.primitives.ciphers.aead import AESGCM 

from .OracleClient import OracleClient

logger = logging.getLogger(__name__)

class ArrowQuery:
    def __init__(self, 
                 client: OracleClient,
                 statement: str = "", 
                 binds: dict = {}
                 ) -> None:
        self.client: OracleClient = client
        self.key: bytes = AESGCM.generate_key(bit_length=256)
        self.statement: str = statement
        self.binds: dict = binds

    def query(self) -> pd.DataFrame:
        odf: oracledb.DataFrame = self.client().fetch_df_all(
            statement=self.statement, parameters=self.binds)
        patable = pa.table(odf)
        buffer = io.BytesIO()
        pq.write_table(patable, buffer)
        parquet_bytes = buffer.getvalue()
        del buffer, patable, odf
        nonce = AESGCM.generate_key(bit_length=96)
        ciphertext = AESGCM(self.key).encrypt(nonce, parquet_bytes, associated_data=None)
        del parquet_bytes
        fd, self.path = tempfile.mkstemp(suffix=".parquet")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(nonce + ciphertext)
        except Exception as e:
            os.unlink(self.path)
            logger.error(f"Failed to write encrypted parquet to disk at {self.path}\nError: {e}")
            raise
        encrypted = os.read(os.open(self.path, os.O_RDONLY), os.path.getsize(self.path))
        plaintext = AESGCM(self.key).decrypt(encrypted[:12], encrypted[12:], associated_data=None)
        return pd.read_parquet(io.BytesIO(plaintext))
    
    def __call__(self, statement: str | None = None, binds: dict | None = None) -> pd.DataFrame:
        if statement is not None:
            self.statement = statement
        if binds is not None:
            self.binds = binds
        return self.query()
    
    def __del__(self):
        os.unlink(self.path)