from unittest.mock import MagicMock

import pytest
from sanctumlabs.messageschema.messages.notifications.email.v1.events_pb2 import EmailV1
from tests import (
    EMAIL_SENT_PROTOBUF_MESSAGE,
    EMAIL_SENT_PROTOBUF_MESSAGE_WITH_SCHEMA_REGISTRY_FRAMING_AS_BYTES,
)
from eventmsg_adaptor.serializers import SerializationError
from eventmsg_adaptor.serializers.confluent_protobuf_serializer import (
    ConfluentProtobufSerializer,
)
from eventmsg_adaptor.schema import (
    Destination,
    SerializationContext,
    SerializationFormat,
)


def test_confluent_protobuf_serializer(
    mock_schema_registry_client: MagicMock,
) -> None:
    serializer = ConfluentProtobufSerializer(mock_schema_registry_client)
    message = serializer.serialize(
        value=EMAIL_SENT_PROTOBUF_MESSAGE,
        serialization_context=SerializationContext(
            destination=Destination(topic="email_sent_v1")
        ),
    )

    assert message.serialization_format == SerializationFormat.PROTOBUF
    assert (
        message.message
        == EMAIL_SENT_PROTOBUF_MESSAGE_WITH_SCHEMA_REGISTRY_FRAMING_AS_BYTES
    )


def test_confluent_protobuf_deserializer() -> None:
    serializer = ConfluentProtobufSerializer()
    vetting_started_protobuf_message = serializer.deserialize(
        EMAIL_SENT_PROTOBUF_MESSAGE_WITH_SCHEMA_REGISTRY_FRAMING_AS_BYTES,
        model=EmailV1,
    )

    assert isinstance(vetting_started_protobuf_message, EmailV1)
    assert vetting_started_protobuf_message == EMAIL_SENT_PROTOBUF_MESSAGE


def test_confluent_protobuf_deserializer_raises_serialization_error_on_failure() -> (
    None
):
    with pytest.raises(SerializationError):
        serializer = ConfluentProtobufSerializer()
        serializer.deserialize(b"this is invalid", model=EmailV1)
