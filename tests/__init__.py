import logging
from datetime import datetime
from typing import Any, Dict, Optional
from unittest.mock import create_autospec
from uuid import UUID, uuid4

from aiokafka import ConsumerRecord
from google.protobuf.timestamp_pb2 import Timestamp
from sanctumlabs.messageschema.events.envelope.v1.envelope_pb2 import EventFields, StandardMessageFields
from sanctumlabs.messageschema.messages.notifications.email.v1.events_pb2 import EmailV1
from sanctumlabs.messageschema.events.notifications.email.v1.events_pb2 import EmailSent
from sanctumlabs.messageschema.events.notifications.email.v1.data_pb2 import Email, EmailStatus

from eventmsg_adaptor.adapters import BaseAdapter
from eventmsg_adaptor.event_streams import EventStream
from eventmsg_adaptor.schema import (
    Destination,
    Event,
    EventHeaders,
    InboundMessage,
    PydanticEventBody,
)
from eventmsg_adaptor.utils import normalise_destination


class MockEventStream(EventStream):
    pass


def make_mock_event_stream(
    adapter: Optional[BaseAdapter] = None, event_source: str = "my-service"
) -> MockEventStream:
    adapter = adapter or create_autospec(BaseAdapter)

    return MockEventStream(adapter=adapter, event_source=event_source)


class BusinessRegistered(PydanticEventBody):
    business_id: str

    @property
    def event_name(self) -> str:
        return "business_registered"


class OrderPlaced(PydanticEventBody):
    order_id: str

    @property
    def event_name(self) -> str:
        return "order_placed"


class OrderPaid(PydanticEventBody):
    order_id: str
    is_test: Optional[bool] = False

    @property
    def event_name(self) -> str:
        return "order_paid"


def make_mock_event(
    event_id: Optional[UUID] = None,
    event_name: str = "order_placed",
    destination: str = "shop.order",
    version: str = "1.0",
    timestamp: Optional[datetime] = None,
    source: str = "my-service",
    body: Optional[PydanticEventBody] = None,
) -> Event:
    return Event(
        headers=EventHeaders(
            id=event_id or uuid4(),
            event_name=event_name,
            destination=normalise_destination(destination),
            version=version,
            timestamp=timestamp or datetime.now(),
            source=source,
        ),
        body=body or OrderPlaced(order_id="17615321"),
    )


# VettingStarted (protobuf)

created_timestamp = Timestamp()
created_timestamp.FromDatetime(datetime(2020, 9, 8, 19, 53, 46))

started_at_timestamp = Timestamp()
started_at_timestamp.FromDatetime(datetime(2023, 7, 31, 16, 20))

EMAIL_SENT_PROTOBUF_MESSAGE = EmailV1(
    event_fields=EventFields(
        message_fields=StandardMessageFields(
            message_uuid="fae9ef15-06f0-46b9-95f1-2a75dd482687",
            created_timestamp=created_timestamp,
        ),
    ),
    email_sent=EmailSent(
        email=Email(
            id="fae9ef15-06f0-46b9-95f1-2a75dd482687",
            to="bot@example.com",
            subject="Testing 123",
            message="Robot Schematics",
            status=EmailStatus.PENDING
        )
    )
)

EMAIL_SENT_PROTOBUF_MESSAGE_AS_BYTES = b"\n0\n.\n$fae9ef15-06f0-46b9-95f1-2a75dd482687\x1a\x06\x08\xca\xc2\xdf\xfa\x05\x12.\n\x06\x08\xb0\xbe\x9f\xa6\x06\x12$2e8a249a-001c-4880-a8b0-0a84b2811a59"
EMAIL_SENT_PROTOBUF_MESSAGE_WITH_SCHEMA_REGISTRY_FRAMING_AS_BYTES = (
    b"\x00\x00\x01\x86\xbc\x00" + EMAIL_SENT_PROTOBUF_MESSAGE_AS_BYTES
)

EMAIL_SENT_EVENT = Event(
    headers=EventHeaders(
        id=UUID("fae9ef15-06f0-46b9-95f1-2a75dd482687"),
        event_name="EmailSent",
        destination=Destination(topic="email_sent_v1"),
        version="1.0",
        timestamp=datetime(2020, 9, 8, 19, 53, 46),
        source="my-service",
    ),
    body=EMAIL_SENT_PROTOBUF_MESSAGE,
)

EMAIL_SENT_KAFKA_HEADERS = [
    ("id", b"fae9ef15-06f0-46b9-95f1-2a75dd482687"),
    ("event_name", b"EmailSent"),
    ("destination", b"email_sent_v1"),
    ("version", b"1.0"),
    ("timestamp", b"2020-09-08T19:53:46"),
    ("source", b"my-service"),
    ("correlation_id", b""),
    ("group_id", b""),
]

EMAIL_SENT_CONSUMER_RECORD = ConsumerRecord(
    topic="email_sent_v1",
    partition=0,
    offset=4,
    timestamp=1688141486475,
    timestamp_type=0,
    key=None,
    value=EMAIL_SENT_PROTOBUF_MESSAGE_AS_BYTES,
    checksum=0,
    serialized_key_size=1,
    serialized_value_size=123,
    headers=EMAIL_SENT_KAFKA_HEADERS,
)

EMAIL_SENT_CONSUMER_RECORD_WITH_SCHEMA_REGISTRY_FRAMING = ConsumerRecord(
    topic="email_sent_v1",
    partition=0,
    offset=4,
    timestamp=1688141486475,
    timestamp_type=0,
    key=None,
    value=EMAIL_SENT_PROTOBUF_MESSAGE_WITH_SCHEMA_REGISTRY_FRAMING_AS_BYTES,
    checksum=0,
    serialized_key_size=1,
    serialized_value_size=123,
    headers=EMAIL_SENT_KAFKA_HEADERS,
)

EMAIL_SENT_INBOUND_MESSAGE = InboundMessage[ConsumerRecord, Dict[str, Any]](
    event=EMAIL_SENT_EVENT,
    raw_message=EMAIL_SENT_CONSUMER_RECORD,
    attributes={},
)

EMAIL_SENT_INBOUND_MESSAGE_WITH_SCHEMA_REGISTRY_FRAMING = InboundMessage[
    ConsumerRecord, Dict[str, Any]
](
    event=EMAIL_SENT_EVENT,
    raw_message=EMAIL_SENT_CONSUMER_RECORD_WITH_SCHEMA_REGISTRY_FRAMING,
    attributes={},
)

# OrderPaid (pydantic)

ORDER_PAID_EVENT_BODY = OrderPaid(order_id="17615321")

ORDER_PAID_EVENT = Event(
    headers=EventHeaders(
        id=UUID("fae9ef15-06f0-46b9-95f1-2a75dd482687"),
        event_name="order_paid",
        destination=Destination(topic="shop", sub_topic="order"),
        version="1.0",
        timestamp=datetime(2020, 9, 8, 19, 53, 46),
        source="my-service",
    ),
    body=ORDER_PAID_EVENT_BODY,
)

ORDER_PAID_INBOUND_MESSAGE = InboundMessage[str, Dict[str, Any]](
    event=ORDER_PAID_EVENT,
    raw_message="foo",
    attributes={},
)

ORDER_PAID_KAFKA_HEADERS = [
    ("id", b"fae9ef15-06f0-46b9-95f1-2a75dd482687"),
    ("event_name", b"order_paid"),
    ("destination", b"shop"),
    ("version", b"1.0"),
    ("timestamp", b"2020-09-08T19:53:46"),
    ("source", b"my-service"),
    ("correlation_id", b""),
    ("group_id", b""),
]

ORDER_PAID_EVENT_BODY_AS_BYTES = b'{"order_id":"17615321","is_test":false}'

ORDER_PAID_CONSUMER_RECORD = ConsumerRecord(
    topic="shop",
    partition=0,
    offset=0,
    timestamp=1688141486475,
    timestamp_type=0,
    key=None,
    value=ORDER_PAID_EVENT_BODY_AS_BYTES,
    checksum=0,
    serialized_key_size=1,
    serialized_value_size=123,
    headers=ORDER_PAID_KAFKA_HEADERS,
)

ORDER_PAID_KAFKA_INBOUND_MESSAGE = InboundMessage[ConsumerRecord, Dict[str, Any]](
    event=ORDER_PAID_EVENT.model_copy(
        update={
            "headers": ORDER_PAID_EVENT.headers.model_copy(
                update={"destination": Destination(topic="shop")}
            )
        }
    ),
    raw_message=ORDER_PAID_CONSUMER_RECORD,
    attributes={},
)

# OrderPlaced (pydantic)

ORDER_PLACED_EVENT_BODY = OrderPlaced(order_id="17615321")

ORDER_PLACED_EVENT = Event(
    headers=EventHeaders(
        id=UUID("fae9ef15-06f0-46b9-95f1-2a75dd482687"),
        event_name="order_placed",
        destination=Destination(topic="shop", sub_topic="order"),
        version="1.0",
        timestamp=datetime(2020, 9, 8, 19, 53, 46),
        source="my-service",
    ),
    body=ORDER_PLACED_EVENT_BODY,
)

ORDER_PLACED_INBOUND_MESSAGE = InboundMessage[str, Dict[str, Any]](
    event=ORDER_PLACED_EVENT,
    raw_message="foo",
    attributes={},
)

# BusinessRegistered (pydantic)

BUSINESS_REGISTERED_EVENT_BODY = BusinessRegistered(business_id="moo")

BUSINESS_REGISTERED_EVENT = Event(
    headers=EventHeaders(
        id=UUID("7eb3e682-5e08-4540-9a74-ff94a43f27e1"),
        event_name="business_registered",
        destination=Destination(
            topic="merchant_profile", sub_topic="business", is_fifo=True
        ),
        version="1.0",
        timestamp=datetime(2020, 9, 8, 19, 53, 46),
        source="my-service",
    ),
    body=BUSINESS_REGISTERED_EVENT_BODY,
)

BUSINESS_REGISTERED_INBOUND_MESSAGE = InboundMessage[str, Dict[str, Any]](
    event=BUSINESS_REGISTERED_EVENT,
    raw_message="foo",
    attributes={},
)

# The following loggers are really noisy, so make sure we never output debug logging.
for module in ["boto3", "botocore", "s3transfer", "urllib3", "aiokafka", "asyncio"]:
    logging.getLogger(module).setLevel(logging.WARNING)
