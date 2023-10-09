from unittest.mock import MagicMock

import pytest

from tests import (
    ORDER_PAID_EVENT_BODY,
    OrderPaid,
)
from eventmsg_adaptor.serializers import SerializationError
from eventmsg_adaptor.serializers.pydantic_serializer import PydanticSerializer
from eventmsg_adaptor.schema import SerializationContext, SerializationFormat


def test_pydantic_serializer() -> None:
    serializer = PydanticSerializer()
    message = serializer.serialize(
        ORDER_PAID_EVENT_BODY, serialization_context=SerializationContext()
    )

    assert message.serialization_format == SerializationFormat.JSON
    assert message.message == b'{"order_id":"17615321","is_test":false}'


def test_pydantic_deserializer() -> None:
    serializer = PydanticSerializer()
    order_paid_event_body = serializer.deserialize(
        b'{"order_id":"17615321","is_test":false}', model=OrderPaid
    )

    assert isinstance(order_paid_event_body, OrderPaid)
    assert order_paid_event_body.order_id == "17615321"
    assert not order_paid_event_body.is_test


def test_pydantic_deserializer_raises_serialization_error_on_failure() -> None:
    with pytest.raises(SerializationError):
        serializer = PydanticSerializer()
        serializer.deserialize(b'{"is_test":false}', model=OrderPaid)
