from sqlmodel import create_engine, Session, SQLModel
from sqlalchemy.pool import NullPool, QueuePool
from sqlalchemy import event
from configs.settings import settings
from server.core.OracleClient import OracleClient
from server.services.session_service import SessionService
import logging

logger = logging.getLogger(__name__)

# Singleton OracleClient instance (manages environment variables & connection lifecycle)
oracle_client = OracleClient()

# Singleton SessionService — auth.py and any route needing session/rate-limit access imports this
session_service = SessionService(oracle_client)

# SQLAlchemy engine using OracleClient as the connection source
# Uses a custom connection creator that delegates to OracleClient
engine = create_engine(
    "oracle+oracledb://",  # Minimal URL; actual connection handled by creator
    echo=settings.DEBUG,
    creator=lambda: oracle_client.get_con(),  # Delegate all connections to OracleClient
    poolclass=NullPool,  # OracleClient handles its own connection lifecycle
)

def init_db():
    """Initialize database: create all tables from SQLModel models."""
    # Important: Import models here so SQLModel knows about them
    from models.user import User
    from models.lead import Lead
    # Note: PluginModels are not SQL-backed; they're in-memory Pydantic models
    SQLModel.metadata.create_all(engine)

def get_session():
    """Dependency for FastAPI endpoints to get a database session."""
    with Session(engine) as session:
        yield session
