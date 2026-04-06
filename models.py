"""
Pydantic schemas for integration events.

The event_type field is a closed Literal -- any value outside this set
is rejected with 422 before the handler runs. This is the schema gate.
"""

from typing import Literal
from pydantic import BaseModel

EventType = Literal[
    "account_created",
    "data_synced",
    "error_occurred",
    "config_updated",
    "webhook_registered",
]


class IntegrationEvent(BaseModel):
    event_type: EventType
    payload: dict
    source: str
