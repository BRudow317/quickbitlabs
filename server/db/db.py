from server.db.ServerDatabase import ServerDatabase
from server.services.session_service import SessionService

# Shared singletons for server operations, not core application functions which should use the plugin framework.
server_db = ServerDatabase()
session_service = SessionService(server_db)
