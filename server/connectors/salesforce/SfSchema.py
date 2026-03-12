from api import SfSession

class SfSchema:
    def __init__(self, session: SfSession):
        self.session = session
        
    def get_database_ddl(self, object_name: str) -> str:
        """Calls the REST describe endpoint and generates a SQL CREATE TABLE string."""
        pass
        
    def validate_schema(self, object_name: str, db_columns: dict) -> list[str]:
        """Compares SF fields against your DB dictionary and returns a list of mismatched types."""
        pass