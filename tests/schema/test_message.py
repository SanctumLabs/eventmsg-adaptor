from eventmsg_adaptor.schema import (
    SerializationFormat,
    SerializedMessage,
)

def test_serialized_message_message_as_str() -> None:
    message = SerializedMessage(
        serialization_format=SerializationFormat.JSON, message=b"hello world"
    )

    message_as_str = message.message_as_str
    assert type(message_as_str) == str
    assert message_as_str == "hello world"
