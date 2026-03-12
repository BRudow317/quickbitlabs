from sqlmodel import create_engine, Session, SQLModel
from server.configs.settings import settings

# sqlite:///./database/pyscript.db
# check_same_thread=False is mandatory for SQLite + FastAPI
engine = create_engine(
    settings.DATABASE_URL, 
    echo=settings.DEBUG, 
    connect_args={"check_same_thread": False} if "sqlite" in settings.DATABASE_URL else {}
)

def init_db():
    # Important: Import models here so SQLModel knows about them
    from server.models.user import User
    from server.models.lead import Lead
    SQLModel.metadata.create_all(engine)

def get_session():
    with Session(engine) as session:
        yield session
