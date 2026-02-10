"""
Lightweight database layer for versioned configs and data quality.

We prefer PostgreSQL in production, but default to a local SQLite file so the
API still works out-of-the-box for demos.
"""

from __future__ import annotations

import os
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker, Session

# In production set e.g.
#   DATABASE_URL=postgresql+psycopg2://user:pass@host:5432/dbname
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./meiro_mmm.db")

engine = create_engine(
    DATABASE_URL,
    echo=False,
    future=True,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, future=True)

Base = declarative_base()


def get_db() -> Generator[Session, None, None]:
    """FastAPI dependency for providing a DB session per-request."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

