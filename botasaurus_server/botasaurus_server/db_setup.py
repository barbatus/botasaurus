import subprocess
import sys
import threading
from contextlib import asynccontextmanager
from os import getcwd, makedirs, path

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import sessionmaker

from .env import is_master
from .server import Server
from .utils import path_task_results, path_task_results_cache, path_task_results_tasks


def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])


def dynamically_import_postgres():
    try:
        import psycopg2  # noqa: F401
    except ImportError:
        install("psycopg2-binary")


def relative_path_backend():
    """Determines the relative path to the database file,
    prioritizing 'backend/db.sqlite3'."""
    if is_master:
        return path.abspath(path.join(getcwd(), "..", "db", "db.sqlite3"))
    else:
        return path.abspath(path.join(getcwd(), "db.sqlite3"))


database_path = relative_path_backend()


def get_sqlite_url():
    return f"sqlite:///{database_path}"


def clean_database_url(url):
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    if url.startswith("postgresql://"):
        dynamically_import_postgres()
    return url


def _make_async_url(url: str) -> str:
    """Convert a regular sync SQLAlchemy URL to its async counterpart."""
    if url.startswith("sqlite:///"):
        # Use aiosqlite driver
        return url.replace("sqlite:///", "sqlite+aiosqlite:///", 1)
    if url.startswith("postgresql://"):
        # Use asyncpg driver
        return url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return url


def _ensure_async_drivers(url: str):
    """Install async database drivers on demand."""
    if url.startswith("sqlite+aiosqlite://"):
        try:
            import aiosqlite  # noqa: F401
        except ImportError:
            install("aiosqlite")
    elif url.startswith("postgresql+asyncpg://"):
        try:
            import asyncpg  # noqa: F401
        except ImportError:
            install("asyncpg")


def ensure_directory_exists(x):
    if not path.exists(x):
        makedirs(x)


ensure_directory_exists(path_task_results)
ensure_directory_exists(path_task_results_tasks)
ensure_directory_exists(path_task_results_cache)

clean_database_url("postgresql://postgres:pgpassword@host.docker.internal:5432/zysk")
db_url = "postgresql://postgres:pgpassword@host.docker.internal:5432/zysk"
Server._is_database_initialized = True
engine = create_engine(
    db_url,
    **(
        Server.database_options
        or {
            "pool_size": 0,
            "max_overflow": 0,
            "pool_pre_ping": True,
        }
    ),
)

Session = sessionmaker(bind=engine)

async_db_url = _make_async_url(db_url)

_ensure_async_drivers(async_db_url)

async_engine = create_async_engine(
    async_db_url,
    **(Server.database_options or {}),
)

AsyncSessionMaker = async_sessionmaker(
    bind=async_engine,
    expire_on_commit=False,
    class_=AsyncSession,
)


thread_local = threading.local()


def create_async_session_maker():
    if not hasattr(thread_local, "async_engine"):
        thread_local.async_engine = create_async_engine(
            async_db_url,
            **(Server.database_options or {}),
        )

        thread_local.session_maker = async_sessionmaker(
            bind=thread_local.async_engine,
            expire_on_commit=False,
            class_=AsyncSession,
        )

    return thread_local.session_maker


@asynccontextmanager
async def get_async_session():
    session_maker = create_async_session_maker()
    async with session_maker() as session:
        yield session


# def create_database():
#     """Creates all tables in the database engine."""
#     Base.metadata.create_all(engine)


# create_database()
