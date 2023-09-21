from typing import Optional, List, Tuple, Dict
from gambol.schema import EventHeaders


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

    return [(header, value.encode("utf-8")) for header, value in kafka_headers.items]
