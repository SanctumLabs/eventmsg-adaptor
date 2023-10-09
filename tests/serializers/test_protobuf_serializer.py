import pytest

from tests import EMAIL_SENT_PROTOBUF_MESSAGE_AS_BYTES, EMAIL_SENT_PROTOBUF_MESSAGE
from sanctumlabs.messageschema.messages.notifications.email.v1.events_pb2 import EmailV1

from eventmsg_adaptor.serializers import SerializationError
from eventmsg_adaptor.serializers.protobuf_serializer import ProtobufSerializer
from eventmsg_adaptor.schema import SerializationContext, SerializationFormat


def test_protobuf_serializer() -> None:
    serializer = ProtobufSerializer()
    message = serializer.serialize(
        EMAIL_SENT_PROTOBUF_MESSAGE, serialization_context=SerializationContext()
    )

    assert message.serialization_format == SerializationFormat.PROTOBUF
    assert message.message == EMAIL_SENT_PROTOBUF_MESSAGE_AS_BYTES


def test_protobuf_deserializer() -> None:
    serializer = ProtobufSerializer()
    email_sent_protobuf_message = serializer.deserialize(
        EMAIL_SENT_PROTOBUF_MESSAGE_AS_BYTES, model=EmailV1
    )

    assert isinstance(email_sent_protobuf_message, EmailV1)
    assert email_sent_protobuf_message == EMAIL_SENT_PROTOBUF_MESSAGE


def test_protobuf_deserializer_raises_serialization_error_on_failure() -> None:
    with pytest.raises(SerializationError):
        serializer = ProtobufSerializer()
        serializer.deserialize(b"this is invalid", model=EmailV1)
