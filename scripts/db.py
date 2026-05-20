"""
db.py

Database connection helper for the retail analytics ELT platform.
Reads connection details from environment variables.
Tests connectivity to the PostgreSQL database and provides a SQLAlchemy engine for use in other scripts.
"""

import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def get_engine(autocommit: bool = False) -> Engine:
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    db = os.environ["POSTGRES_DB"]
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"

    engine = create_engine(url, isolation_level="AUTOCOMMIT" if autocommit else None)
    return engine


def test_connection() -> bool:
    """Return True if the database is reachable, False otherwise."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("[db] Connection successful.")
        return True
    except Exception as e:
        print(f"[db] Connection failed: {e}")
        return False


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    test_connection()
