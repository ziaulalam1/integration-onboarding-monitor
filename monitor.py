"""
Structured logging and SQLite persistence for integration events.

Every inbound event produces one JSON log line on stdout:
  {"customer_id": "...", "event_type": "...", "source": "...",
   "timestamp": "...", "status": "ok|error", "error": "..."}

SQLite is used for the demo. In production this would be PostgreSQL
with connection pooling via asyncpg.
"""

import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

_DEFAULT_DB = Path(__file__).parent / "events.db"


def _init_db(path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(path))
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id TEXT NOT NULL,
            event_type  TEXT NOT NULL,
            source      TEXT NOT NULL,
            timestamp   TEXT NOT NULL
        )
        """
    )
    con.commit()
    return con


def log_event(
    customer_id: str,
    event_type: str,
    source: str,
    status: str,
    error: Optional[str] = None,
) -> dict:
    """Emit one structured JSON log line and return the record."""
    record: dict = {
        "customer_id": customer_id,
        "event_type": event_type,
        "source": source,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": status,
    }
    if error:
        record["error"] = error
    print(json.dumps(record), flush=True)
    return record


def store_event(
    customer_id: str,
    event_type: str,
    source: str,
    db_path: Path = _DEFAULT_DB,
) -> None:
    """Persist a validated event to the database."""
    con = _init_db(db_path)
    con.execute(
        "INSERT INTO events (customer_id, event_type, source, timestamp) VALUES (?, ?, ?, ?)",
        (customer_id, event_type, source, datetime.now(timezone.utc).isoformat()),
    )
    con.commit()
    con.close()


def event_count(customer_id: str, db_path: Path = _DEFAULT_DB) -> int:
    """Return the number of stored events for a given customer."""
    con = _init_db(db_path)
    row = con.execute(
        "SELECT COUNT(*) FROM events WHERE customer_id = ?", (customer_id,)
    ).fetchone()
    con.close()
    return row[0]
