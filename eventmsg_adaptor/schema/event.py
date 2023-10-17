"""Contains Event classes that represent an event in the stream
"""
from __future__ import annotations

from datetime import datetime
from typing import Generic, Optional, TypeVar, Union
from uuid import UUID

from google.protobuf.message import Message
from pydantic import BaseModel

from .destination import Destination


class PydanticEventBody(BaseModel):
    """Represents a Pydantic Event Body"""

    class Config:
        """Extra configuration specific to Pydantic allowing extra types to be added"""

        extra = "allow"

    @property
    def event_name(self) -> str:
        """Event Name

        Returns:
            str: Event name
        """
        raise NotImplementedError()

    @property
    def version(self) -> str:
        """Version of the event body

        Returns:
            str: string version
        """
        return "1.0"


EventBody = Union[PydanticEventBody, Message]


class EventHeaders(BaseModel):
    """Represents the headers on an Event in the stream"""

    id: UUID
    event_name: str
    destination: Destination
    version: str
    timestamp: datetime
    source: Optional[str]
    correlation_id: Optional[str] = None
    group_id: Optional[str] = None
    subject: Optional[str] = None
    signature: Optional[str] = None


EventBody_T = TypeVar("EventBody_T", bound=EventBody)


class Event(BaseModel, Generic[EventBody_T]):
    """Represents the Event object in a stream"""

    headers: EventHeaders
    body: EventBody_T

    class Config:
        """Configuration that allows adding attributes to the event"""

        arbitrary_types_allowed = True

    @property
    def payload(self) -> EventBody_T:
        """Returns the body of the event.

        Returns:
            EventBody_T: body of event
        """
        return self.body
