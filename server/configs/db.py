from server.core.OracleClient import OracleClient
from server.services.session_service import SessionService

# Shared singletons — import these wherever Oracle access or session management is needed
oracle_client = OracleClient()
session_service = SessionService(oracle_client)
