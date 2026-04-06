"""
Tests for Integration Onboarding Monitor.

Two invariants under test:
1. Correctness/reliability -- malformed payloads are rejected with 422
   before store_event is ever called (schema gate holds 100%).
2. Observability -- every log record contains the required fields.
"""

import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import monitor
from main import app

client = TestClient(app, raise_server_exceptions=True)

VALID_EVENT = {
    "event_type": "account_created",
    "payload": {"org_id": "org-001", "plan": "starter"},
    "source": "webhook",
}


# ── API contract ──────────────────────────────────────────────────────────────

def test_valid_event_returns_201():
    r = client.post("/integrations/cust-001/events", json=VALID_EVENT)
    assert r.status_code == 201


def test_response_includes_customer_id():
    r = client.post("/integrations/cust-002/events", json=VALID_EVENT)
    assert r.json()["customer_id"] == "cust-002"


def test_response_includes_status_accepted():
    r = client.post("/integrations/cust-003/events", json=VALID_EVENT)
    assert r.json()["status"] == "accepted"


def test_response_includes_event_type():
    r = client.post("/integrations/cust-004/events", json=VALID_EVENT)
    assert r.json()["event_type"] == "account_created"


# ── Schema validation invariant (correctness gate) ────────────────────────────

def test_missing_event_type_returns_422():
    r = client.post("/integrations/cust-001/events", json={"payload": {}, "source": "webhook"})
    assert r.status_code == 422


def test_invalid_event_type_returns_422():
    r = client.post("/integrations/cust-001/events", json={**VALID_EVENT, "event_type": "not_a_real_type"})
    assert r.status_code == 422


def test_missing_source_returns_422():
    r = client.post("/integrations/cust-001/events", json={"event_type": "account_created", "payload": {}})
    assert r.status_code == 422


def test_missing_payload_returns_422():
    r = client.post("/integrations/cust-001/events", json={"event_type": "account_created", "source": "webhook"})
    assert r.status_code == 422


def test_empty_body_returns_422():
    r = client.post("/integrations/cust-001/events", json={})
    assert r.status_code == 422


def test_all_bad_payloads_rejected_before_handler(monkeypatch):
    """100% of malformed payloads are rejected by the schema gate.
    store_event must never be called for any of them.
    """
    writes = []
    monkeypatch.setattr(monitor, "store_event", lambda *a, **kw: writes.append(a))

    bad_payloads = [
        {},
        {"event_type": "INVALID"},
        {"source": "webhook"},
        {"payload": {"x": 1}},
        {"event_type": "bad_type", "payload": {}, "source": "webhook"},
        {"event_type": 123, "payload": {}, "source": "webhook"},
        {"event_type": "account_created", "payload": "not_a_dict", "source": "webhook"},
        {"event_type": None, "payload": {}, "source": "webhook"},
    ]

    for p in bad_payloads:
        client.post("/integrations/cust-gate/events", json=p)

    assert len(writes) == 0, (
        f"store_event was called {len(writes)} time(s) -- schema gate did not hold"
    )


# ── Observability: log structure ──────────────────────────────────────────────

def test_log_record_contains_required_fields():
    record = monitor.log_event("cust-log", "data_synced", "api", "ok")
    for field in ("customer_id", "event_type", "source", "timestamp", "status"):
        assert field in record, f"Missing field: {field}"


def test_log_record_customer_id_matches():
    record = monitor.log_event("cust-xyz", "data_synced", "api", "ok")
    assert record["customer_id"] == "cust-xyz"


def test_log_record_error_field_included_on_error():
    record = monitor.log_event("cust-err", "error_occurred", "webhook", "error", error="timeout")
    assert record.get("error") == "timeout"


# ── DB persistence ────────────────────────────────────────────────────────────

def test_store_event_increments_count():
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "test.db"
        monitor.store_event("cust-db", "data_synced", "webhook", db_path=db)
        assert monitor.event_count("cust-db", db_path=db) == 1


def test_store_event_isolates_by_customer():
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "test.db"
        monitor.store_event("cust-a", "data_synced", "webhook", db_path=db)
        monitor.store_event("cust-a", "config_updated", "api", db_path=db)
        monitor.store_event("cust-b", "account_created", "webhook", db_path=db)
        assert monitor.event_count("cust-a", db_path=db) == 2
        assert monitor.event_count("cust-b", db_path=db) == 1
