import datetime
from typing import Any
from unittest.mock import ANY, MagicMock, call, create_autospec
from uuid import UUID

from tests import (
    ORDER_PAID_EVENT,
    ORDER_PAID_EVENT_BODY,
    ORDER_PAID_INBOUND_MESSAGE,
    ORDER_PLACED_INBOUND_MESSAGE,
)
from eventmsg_adaptor.adapters import BaseAdapter
from eventmsg_adaptor.event_streams import EventStream
from eventmsg_adaptor.schema import (
    Destination,
    Event,
    EventHeaders,
    ListenerCallback,
    ListenExpression,
    PydanticEventBody,
)


def test_publish() -> None:
    adapter = create_autospec(BaseAdapter)
    publish_result = MagicMock()
    adapter.publish = MagicMock(return_value=publish_result)

    event_stream = EventStream(adapter=adapter, event_source="my-service")

    result = event_stream.publish(
        destination=Destination(topic="shop", sub_topic="order"),
        event_body=ORDER_PAID_EVENT_BODY,
    )

    assert result is publish_result

    adapter.publish.assert_called_once_with(
        destination=Destination(topic="shop", sub_topic="order"),
        event=ORDER_PAID_EVENT,
    )


def test_publish_normalises_destination() -> None:
    adapter = create_autospec(BaseAdapter)
    publish_result = MagicMock()
    adapter.publish = MagicMock(return_value=publish_result)

    event_stream = EventStream(adapter=adapter, event_source="my-service")

    result = event_stream.publish(
        destination="shop.order", event_body=ORDER_PAID_EVENT_BODY
    )

    assert result is publish_result

    adapter.publish.assert_called_once_with(
        destination=Destination(topic="shop", sub_topic="order"),
        event=ORDER_PAID_EVENT,
    )


def test_publish_uses_custom_header_values_when_passed_in() -> None:
    adapter = create_autospec(BaseAdapter)
    publish_result = MagicMock()
    adapter.publish = MagicMock(return_value=publish_result)

    event_stream = EventStream(adapter=adapter, event_source="my-service")

    result = event_stream.publish(
        destination="shop.order::fifo",
        event_body=ORDER_PAID_EVENT_BODY,
        timestamp=datetime.datetime(2018, 3, 20, 12, 45, 19),
        correlation_id="foo",
        group_id="49e6dc6e-aa9a-48e7-9afd-1bcc93804f41",
        subject="675b7f42-ecb9-4764-bf53-0bb0793f14dd",
    )

    assert result is publish_result

    destination = Destination(topic="shop", sub_topic="order", is_fifo=True)
    event = Event(
        headers=EventHeaders(
            id=UUID("fae9ef15-06f0-46b9-95f1-2a75dd482687"),
            event_name="order_paid",
            destination=destination,
            version="1.0",
            timestamp=datetime.datetime(2018, 3, 20, 12, 45, 19),
            source="my-service",
            correlation_id="foo",
            group_id="49e6dc6e-aa9a-48e7-9afd-1bcc93804f41",
            subject="675b7f42-ecb9-4764-bf53-0bb0793f14dd",
        ),
        body=ORDER_PAID_EVENT_BODY,
    )

    adapter.publish.assert_called_once_with(destination=destination, event=event)


def test_publish_raw() -> None:
    adapter = create_autospec(BaseAdapter)
    publish_result = MagicMock()
    adapter.publish = MagicMock(return_value=publish_result)

    event_stream = EventStream(adapter=adapter, event_source="my-service")

    destination = Destination(topic="shop", sub_topic="order")

    result = event_stream.publish_raw(
        destination=Destination(topic="shop", sub_topic="order"),
        event=ORDER_PAID_EVENT,
    )

    assert result is publish_result

    adapter.publish.assert_called_once_with(
        destination=destination, event=ORDER_PAID_EVENT
    )


def test_publish_raw_normalises_destination() -> None:
    adapter = create_autospec(BaseAdapter)
    publish_result = MagicMock()
    adapter.publish = MagicMock(return_value=publish_result)

    event_stream = EventStream(adapter=adapter, event_source="my-service")

    result = event_stream.publish_raw(destination="shop.order", event=ORDER_PAID_EVENT)

    assert result is publish_result

    adapter.publish.assert_called_once_with(
        destination=Destination(topic="shop", sub_topic="order"),
        event=ORDER_PAID_EVENT,
    )


def test_subscribe_handles_a_single_message() -> None:
    def emit_mock_events(callback: ListenerCallback, *args: Any, **kwargs: Any) -> None:
        callback(ORDER_PAID_INBOUND_MESSAGE)

    adapter = create_autospec(BaseAdapter)
    adapter.subscribe = MagicMock(side_effect=emit_mock_events)
    adapter.ack = MagicMock()

    event_stream = EventStream(adapter=adapter, event_source="my-service")

    subscribe_callback = MagicMock()

    event_stream.subscribe("shop.*")(subscribe_callback)

    adapter.subscribe.assert_called_once_with(
        listen_expression=ListenExpression(destination=Destination(topic="shop")),
        callback=ANY,
        event_body_type=PydanticEventBody,
    )

    subscribe_callback.assert_called_once_with(ORDER_PAID_INBOUND_MESSAGE.event)

    adapter.ack.assert_called_once_with(ORDER_PAID_INBOUND_MESSAGE)


def test_subscribe_handles_multiple_messages() -> None:
    def emit_mock_events(callback: ListenerCallback, *args: Any, **kwargs: Any) -> None:
        callback(ORDER_PAID_INBOUND_MESSAGE)
        callback(ORDER_PLACED_INBOUND_MESSAGE)

    adapter = create_autospec(BaseAdapter)
    adapter.subscribe = MagicMock(side_effect=emit_mock_events)
    adapter.ack = MagicMock()

    event_stream = EventStream(adapter=adapter, event_source="my-service")

    event_callback = MagicMock()

    event_stream.subscribe("shop.*")(event_callback)

    adapter.subscribe.assert_called_once_with(
        listen_expression=ListenExpression(destination=Destination(topic="shop")),
        callback=ANY,
        event_body_type=PydanticEventBody,
    )

    event_callback.assert_has_calls(
        [
            call(ORDER_PAID_INBOUND_MESSAGE.event),
            call(ORDER_PLACED_INBOUND_MESSAGE.event),
        ]
    )

    adapter.ack.assert_has_calls(
        [call(ORDER_PAID_INBOUND_MESSAGE), call(ORDER_PLACED_INBOUND_MESSAGE)]
    )


def test_subscribe_does_not_propagate_event_when_listen_expression_does_not_match() -> (
    None
):
    def emit_mock_events(callback: ListenerCallback, *args: Any, **kwargs: Any) -> None:
        callback(ORDER_PAID_INBOUND_MESSAGE)

    adapter = create_autospec(BaseAdapter)
    adapter.subscribe = MagicMock(side_effect=emit_mock_events)
    adapter.ack = MagicMock()

    event_stream = EventStream(adapter=adapter, event_source="my-service")

    subscribe_callback = MagicMock()

    event_stream.subscribe("merchant_profile.*")(subscribe_callback)

    adapter.subscribe.assert_called_once_with(
        listen_expression=ListenExpression(
            destination=Destination(topic="merchant_profile")
        ),
        callback=ANY,
        event_body_type=PydanticEventBody,
    )

    subscribe_callback.assert_not_called()

    adapter.ack.assert_called_once_with(ORDER_PAID_INBOUND_MESSAGE)


def test_subscribe_handles_exceptions_gracefully_when_subscribe_callback_raises_an_exception() -> (
    None
):
    def emit_mock_events(callback: ListenerCallback, *args: Any, **kwargs: Any) -> None:
        callback(ORDER_PAID_INBOUND_MESSAGE)

    adapter = create_autospec(BaseAdapter)
    adapter.subscribe = MagicMock(side_effect=emit_mock_events)
    adapter.nack = MagicMock()

    event_stream = EventStream(adapter=adapter, event_source="my-service")

    subscribe_callback = MagicMock(
        side_effect=Exception("This exception is thrown by the subscriber callback.")
    )

    event_stream.subscribe("shop.*")(subscribe_callback)

    adapter.subscribe.assert_called_once_with(
        listen_expression=ListenExpression(destination=Destination(topic="shop")),
        callback=ANY,
        event_body_type=PydanticEventBody,
    )

    subscribe_callback.assert_called_once_with(ORDER_PAID_INBOUND_MESSAGE.event)

    adapter.nack.assert_called_once_with(ORDER_PAID_INBOUND_MESSAGE)
