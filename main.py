"""
Integration Onboarding Monitor -- FastAPI entry point.

POST /integrations/{customer_id}/events

Pydantic validates the request body before this handler is called.
Any payload with an invalid schema never reaches store_event -- that is
the correctness invariant the tests prove.
"""

from datetime import datetime, timezone

from fastapi import FastAPI

from models import IntegrationEvent
from monitor import log_event, store_event

app = FastAPI(title="Integration Onboarding Monitor")


@app.post("/integrations/{customer_id}/events", status_code=201)
def ingest_event(customer_id: str, event: IntegrationEvent):
    timestamp = datetime.now(timezone.utc).isoformat()
    log_event(customer_id, event.event_type, event.source, "ok")
    store_event(customer_id, event.event_type, event.source)
    return {
        "customer_id": customer_id,
        "event_type": event.event_type,
        "status": "accepted",
        "timestamp": timestamp,
    }
