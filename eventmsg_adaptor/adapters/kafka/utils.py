from typing import Optional, List, Tuple, Dict, Sequence, Any
from uuid import UUID
from aiokafka import ConsumerRecord
from google.protobuf.message import Message
from sanctumlabs.messageschema.events.envelope.v1.envelope_pb2 import EventFields
from eventmsg_adaptor.schema import EventHeaders, EventBody, PydanticEventBody
from eventmsg_adaptor.utils import (
    parse_event_name_and_version_from_protobuf_message,
    normalise_destination,
)


def key_serializer(value: Optional[str]) -> bytes:
    """Serializes a key to bytes

    Args:
        value (Optional[str]): optional key value

    Returns:
        bytes: serialized key as bytes
    """
    return value.encode("utf-8") if value else b""


def make_kafka_headers_from_event_headers(
    headers: EventHeaders,
) -> List[Tuple[str, bytes]]:
    """Makes kafka headers given event headers. Re

    Args:
        headers (EventHeaders): event headers

    Returns:
        List[Tuple[str, bytes]]: list of kafka headers
    """
    kafka_headers: Dict[str, str] = {
        "id": str(headers.id),
        "event_name": headers.event_name,
        "destination": headers.destination.topic,
        "version": headers.version,
        "timestamp": headers.timestamp.isoformat(),
        "source": headers.source or "",
        "correlation_id": headers.correlation_id or "",
        "group_id": headers.group_id or "",
    }

    return [(header, value.encode("utf-8")) for header, value in kafka_headers.items()]


def parse_event_headers(event_body: EventBody, message: ConsumerRecord) -> EventHeaders:
    """Parses the event body and message to construct even headers

    Args:
        event_body (EventBody): Event Body
        message (ConsumerRecord): Message

    Raises:
        Exception: when the message does not have event_fields attribute
        Exception: if the message type is not supported

    Returns:
        EventHeaders: parsed event headers from the message and the body
    """
    if isinstance(event_body, Message):
        event_name, version = parse_event_name_and_version_from_protobuf_message(
            event_body
        )

        if not hasattr(event_body, "event_fields"):
            raise Exception(
                "The message does not appear to be a sanctumlabs-proto type message"
            )

        event_fields: EventFields = getattr(event_body, "event_fields")

        # all header values, except for 'source' can be unpicked from the protobuf message itself
        parsed_kafka_headers = parse_kafka_headers(message.headers)

        return EventHeaders(
            id=UUID(event_fields.message_fields.message_uuid),
            event_name=event_name,
            destination=normalise_destination(message.topic),
            version=version,
            timestamp=event_fields.message_fields.created_timestamp.ToDatetime(),
            source=parsed_kafka_headers.get("source") or None,
            correlation_id=event_fields.message_fields.correlation_uuid or None,
            group_id=event_fields.aggregate_id or None,
        )
    elif isinstance(event_body, PydanticEventBody):
        parsed_kafka_headers = parse_kafka_headers(message.headers)

        if "destination" in parsed_kafka_headers:
            parsed_kafka_headers["destination"] = normalise_destination(  # type: ignore[assignment]
                parsed_kafka_headers["destination"]
            )

        return EventHeaders.model_validate(parsed_kafka_headers)
    else:
        raise Exception(f"The type {type(event_body)} is not supported")


def parse_kafka_headers(headers: Sequence[Tuple[str, bytes]]) -> Dict[str, str]:
    """Parses Kafka headers and returns a dictionary mapping of the header to the value. The value is decoded to a string

    Args:
        headers (Sequence[Tuple[str, bytes]]): sequence of kafka headers

    Returns:
        Dict[str, str]: Key value mapping of header to value, if value is missing, it will be None
    """
    parsed_kafka_headers: Dict[str, Any] = {}

    for header, value_as_bytes in headers:
        value: Optional[str] = value_as_bytes.decode("utf-8")

        if value == "":
            value = None

        parsed_kafka_headers[header] = value

    return parsed_kafka_headers
