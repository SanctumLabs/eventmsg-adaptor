from __future__ import annotations

import logging
import uuid
from abc import ABC
from datetime import datetime
from typing import Optional

from google.protobuf.message import Message
from google.protobuf.timestamp_pb2 import Timestamp
from sanctumlabs.messageschema.events.envelope.v1.envelope_pb2 import (
    EventFields,
    StandardMessageFields,
)

from eventmsg_adaptor.schema import (
    Destination,
    Event,
    EventBody,
    EventHeaders,
    PydanticEventBody,
)
from eventmsg_adaptor.utils import (
    normalise_destination,
    parse_event_name_and_version_from_protobuf_message,
)

logger = logging.getLogger(__name__)


class BaseEventStream(ABC):
    def __init__(self, event_source: Optional[str]):
        self.event_source: Optional[str] = event_source

    def _make_outbound_event(
        self,
        destination: Destination,
        event_body: EventBody,
        event_id: Optional[uuid.UUID] = None,
        timestamp: Optional[datetime] = None,
        correlation_id: Optional[str] = None,
        group_id: Optional[str] = None,
        subject: Optional[str] = None,
    ) -> Event:
        """Creates an outbound event given the arguments

        Args:
            destination (Destination): Destination of event
            event_body (EventBody): Body of event
            event_id (Optional[uuid.UUID], optional): Event iD Defaults to None.
            timestamp (Optional[datetime], optional): Timestamp of event. Defaults to None.
            correlation_id (Optional[str], optional): Correlation ID. Defaults to None.
            group_id (Optional[str], optional): Group ID this event is for. Defaults to None.
            subject (Optional[str], optional): Subject of event. Defaults to None.

        Returns:
            Event: Outbound Event
        """
        destination = normalise_destination(destination)

        if isinstance(event_body, Message):
            return self._make_protobuf_outbound_message(
                destination=destination,
                message=event_body,
                event_id=event_id,
                timestamp=timestamp,
                correlation_id=correlation_id,
                group_id=group_id,
            )
        elif isinstance(event_body, PydanticEventBody):
            return self._make_pydantic_outbound_event(
                destination=destination,
                event_name=event_body.event_name,
                version=event_body.version,
                event_body=event_body,
                event_id=event_id,
                timestamp=timestamp,
                correlation_id=correlation_id,
                group_id=group_id,
                subject=subject,
            )
        else:
            raise Exception(f"The type {type(event_body)} is not supported")

    def _make_protobuf_outbound_message(
        self,
        destination: Destination,
        message: Message,
        event_id: Optional[uuid.UUID] = None,
        timestamp: Optional[datetime] = None,
        correlation_id: Optional[str] = None,
        group_id: Optional[str] = None,
    ) -> Event:
        """Makes a protobuf type of event given the arguments.

        Args:
            destination (Destination): Destination of event
            message (Message): message payload
            event_id (Optional[uuid.UUID], optional): ID of event. Defaults to None.
            timestamp (Optional[datetime], optional): timestamp of the event. Defaults to None.
            correlation_id (Optional[str], optional): correlation ID. Defaults to None.
            group_id (Optional[str], optional): Group ID for this event. Defaults to None.

        Returns:
            Event: Protobuf event
        """
        # We don't have a protobuf message, but since we can't extend a base class in message-schema, we need to make sure
        # that it actually looks like an event type of message.

        if not hasattr(message, "event_fields"):
            raise Exception(
                "Are you sure you passed in a sanctumlabs-messageschema type message?"
            )

        event_fields: EventFields = getattr(message, "event_fields")

        event_name, version = parse_event_name_and_version_from_protobuf_message(
            message=message
        )

        # event id
        if event_fields.message_fields.message_uuid:
            final_event_id = uuid.UUID(event_fields.message_fields.message_uuid)
        else:
            final_event_id = event_id or uuid.uuid4()

        # timestamp
        if (
            event_fields.message_fields.created_timestamp
            and event_fields.message_fields.created_timestamp.seconds
        ):
            final_timestamp = event_fields.message_fields.created_timestamp.ToDatetime()
        else:
            final_timestamp = timestamp or datetime.now()

        # correlation ID
        final_correlation_id = (
            event_fields.message_fields.correlation_uuid or correlation_id
        )

        # group ID
        final_group_id = event_fields.aggregate_id or group_id

        # event headers
        headers = EventHeaders(
            id=final_event_id,
            event_name=event_name,
            destination=destination,
            version=version,
            timestamp=final_timestamp,
            source=self.event_source,
            correlation_id=final_correlation_id,
            group_id=final_group_id,
        )

        updated_message = message.__class__()
        updated_message.CopyFrom(message)

        updated_protobuf_timestamp = Timestamp()
        updated_protobuf_timestamp.FromDatetime(final_timestamp)
        updated_event_fields = EventFields(
            message_fields=StandardMessageFields(
                message_uuid=str(final_event_id) if final_event_id else "",
                correlation_uuid=str(final_correlation_id)
                if final_correlation_id
                else "",
                created_timestamp=updated_protobuf_timestamp,
            ),
            aggregate_id=str(final_group_id) if final_group_id else "",
        )

        updated_message.event_fields.MergeFrom(updated_event_fields)  # type: ignore[attr-defined]

        return Event(headers=headers, body=updated_message)

    def _make_pydantic_outbound_event(
        self,
        destination: Destination,
        event_name: str,
        version: str,
        event_body: PydanticEventBody,
        event_id: Optional[uuid.UUID] = None,
        timestamp: Optional[datetime] = None,
        correlation_id: Optional[str] = None,
        group_id: Optional[str] = None,
        subject: Optional[str] = None,
    ) -> Event:
        """Makes a Pydantic Outbound event from the given arguments

        Args:
            destination (Destination): Destination of event
            event_name (str): name of event
            version (str): event version.
            event_body (PydanticEventBody): Body of event, in this case the pydantic event body
            event_id (Optional[uuid.UUID], optional): event ID. Defaults to None.
            timestamp (Optional[datetime], optional): timestamp of event. Defaults to None.
            correlation_id (Optional[str], optional): Correlation ID. Defaults to None.
            group_id (Optional[str], optional): Group ID. Defaults to None.
            subject (Optional[str], optional): subject of event. Defaults to None.

        Returns:
            Event: Generated Event
        """
        return Event(
            headers=EventHeaders(
                id=event_id or uuid.uuid4(),
                event_name=event_name,
                destination=destination,
                version=version,
                timestamp=timestamp or datetime.now(),
                source=self.event_source,
                correlation_id=correlation_id,
                group_id=group_id,
                subject=subject,
            ),
            body=event_body,
        )
