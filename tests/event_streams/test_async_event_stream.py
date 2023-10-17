import asyncio
import datetime
from asyncio import BaseEventLoop, Task
from typing import Any, List, cast
from unittest.mock import AsyncMock, MagicMock, call, create_autospec
from uuid import UUID

import pytest
from google.protobuf.message import Message
from google.protobuf.timestamp_pb2 import Timestamp
from sanctumlabs.messageschema.events.envelope.v1.envelope_pb2 import (
    EventFields,
    StandardMessageFields,
)
from sanctumlabs.messageschema.events.notifications.email.v1.events_pb2 import EmailSent
from sanctumlabs.messageschema.messages.notifications.email.v1.events_pb2 import EmailV1
from sanctumlabs.messageschema.events.notifications.email.v1.data_pb2 import (
    Email,
    EmailStatus,
)


from tests import (
    BUSINESS_REGISTERED_INBOUND_MESSAGE,
    ORDER_PAID_EVENT,
    ORDER_PAID_EVENT_BODY,
    ORDER_PAID_INBOUND_MESSAGE,
    ORDER_PLACED_EVENT,
    ORDER_PLACED_EVENT_BODY,
    ORDER_PLACED_INBOUND_MESSAGE,
    EMAIL_SENT_EVENT,
    EMAIL_SENT_PROTOBUF_MESSAGE,
)
from eventmsg_adaptor.adapters import BaseAsyncAdapter
from eventmsg_adaptor.event_streams import AsyncEventStream
from eventmsg_adaptor.schema import (
    AsyncListener,
    Destination,
    Event,
    EventHeaders,
    InboundMessage,
    ListenerState,
    ListenExpression,
    PydanticEventBody,
)


@pytest.mark.asyncio
async def test_instance_uses_default_event_loop_when_not_given() -> None:
    adapter = create_autospec(BaseAsyncAdapter)
    event_stream = AsyncEventStream(adapter=adapter, event_source="kyc")
    assert event_stream.event_loop is asyncio.get_running_loop()


def test_event_loop_is_resolved_from_callable_if_callable() -> None:
    event_loop: BaseEventLoop = create_autospec(BaseEventLoop)

    def make_event_loop() -> BaseEventLoop:
        return event_loop

    adapter = create_autospec(BaseAsyncAdapter)
    event_stream = AsyncEventStream(
        adapter=adapter, event_source="kyc", event_loop=make_event_loop
    )
    assert event_stream.event_loop is event_loop


@pytest.mark.asyncio
async def test_publish_event_with_pydantic_event_body() -> None:
    adapter = create_autospec(BaseAsyncAdapter)
    publish_result = MagicMock()
    adapter.publish = AsyncMock(return_value=publish_result)

    event_stream = AsyncEventStream(adapter=adapter, event_source="my-service")

    result = await event_stream.publish(
        destination=Destination(topic="shop", sub_topic="order"),
        event_body=ORDER_PAID_EVENT_BODY,
    )

    assert result is publish_result

    adapter.publish.assert_awaited_once_with(
        destination=Destination(topic="shop", sub_topic="order"),
        event=ORDER_PAID_EVENT,
    )


@pytest.mark.asyncio
async def test_publish_event_with_protobuf_message_body() -> None:
    adapter = create_autospec(BaseAsyncAdapter)
    publish_result = MagicMock()
    adapter.publish = AsyncMock(return_value=publish_result)

    event_stream = AsyncEventStream(adapter=adapter, event_source="my-service")

    result = await event_stream.publish(
        destination=Destination(topic="email_sent_v1"),
        event_body=EMAIL_SENT_PROTOBUF_MESSAGE,
    )

    assert result is publish_result

    adapter.publish.assert_awaited_once_with(
        destination=Destination(topic="email_sent_v1"),
        event=EMAIL_SENT_EVENT,
    )


@pytest.mark.asyncio
async def test_publish_event_with_protobuf_message_body_raises_an_exception_when_message_is_not_a_messageschema_proto_type_message() -> None:
    adapter = create_autospec(BaseAsyncAdapter)
    publish_result = MagicMock()
    adapter.publish = AsyncMock(return_value=publish_result)

    event_stream = AsyncEventStream(adapter=adapter, event_source="my-service")

    with pytest.raises(
        Exception,
        match="Are you sure you passed in a sanctumlabs-messageschema type message?",
    ):
        await event_stream.publish(
            destination=Destination(topic="email_sent_v1"),
            event_body=Message(),
        )


@pytest.mark.asyncio
async def test_publish_event_with_protobuf_message_body_updates_headers_any_body_when_custom_header_values_are_passed_in() -> None:
    adapter = create_autospec(BaseAsyncAdapter)
    publish_result = MagicMock()
    adapter.publish = AsyncMock(return_value=publish_result)

    event_stream = AsyncEventStream(adapter=adapter, event_source="my-service")

    result = await event_stream.publish(
        destination=Destination(topic="email_sent_v1"),
        event_body=EMAIL_SENT_PROTOBUF_MESSAGE,
        correlation_id="foobar",
        group_id="12121212",
    )

    assert result is publish_result

    created_timestamp = Timestamp()
    created_timestamp.FromDatetime(datetime.datetime(2020, 9, 8, 19, 53, 46))

    started_at_timestamp = Timestamp()
    started_at_timestamp.FromDatetime(datetime.datetime(2023, 7, 31, 16, 20))

    adapter.publish.assert_awaited_once_with(
        destination=Destination(topic="email_sent_v1"),
        event=Event(
            headers=EventHeaders(
                id=UUID("fae9ef15-06f0-46b9-95f1-2a75dd482687"),
                event_name="EmailSent",
                destination=Destination(topic="email_sent_v1"),
                version="1.0",
                timestamp=datetime.datetime(2020, 9, 8, 19, 53, 46),
                source="my-service",
                correlation_id="foobar",
                group_id="12121212",
            ),
            body=EmailV1(
                event_fields=EventFields(
                    message_fields=StandardMessageFields(
                        message_uuid="fae9ef15-06f0-46b9-95f1-2a75dd482687",
                        correlation_uuid="foobar",
                        created_timestamp=created_timestamp,
                    ),
                    aggregate_id="12121212",
                ),
                email_sent=EmailSent(
                    email=Email(
                        id="fae9ef15-06f0-46b9-95f1-2a75dd482687",
                        to="bot@example.com",
                        subject="Testing 123",
                        message="Robot Schematics",
                        status=EmailStatus.PENDING,
                    )
                ),
            ),
        ),
    )


@pytest.mark.asyncio
async def test_publish_normalises_destination() -> None:
    adapter = create_autospec(BaseAsyncAdapter)
    publish_result = MagicMock()
    adapter.publish = AsyncMock(return_value=publish_result)

    event_stream = AsyncEventStream(adapter=adapter, event_source="my-service")

    result = await event_stream.publish(
        destination="shop.order",
        event_body=ORDER_PAID_EVENT_BODY,
    )

    assert result is publish_result

    adapter.publish.assert_awaited_once_with(
        destination=Destination(topic="shop", sub_topic="order"),
        event=ORDER_PAID_EVENT,
    )


@pytest.mark.asyncio
async def test_publish_uses_custom_header_values_when_passed_in() -> None:
    adapter = create_autospec(BaseAsyncAdapter)
    publish_result = MagicMock()
    adapter.publish = AsyncMock(return_value=publish_result)

    event_stream = AsyncEventStream(adapter=adapter, event_source="my-service")

    result = await event_stream.publish(
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

    adapter.publish.assert_awaited_once_with(destination=destination, event=event)


@pytest.mark.asyncio
async def test_publish_raw() -> None:
    adapter = create_autospec(BaseAsyncAdapter)
    publish_result = MagicMock()
    adapter.publish = AsyncMock(return_value=publish_result)

    event_stream = AsyncEventStream(adapter=adapter, event_source="my-service")

    destination = Destination(topic="shop", sub_topic="order")

    result = await event_stream.publish_raw(
        destination=destination, event=ORDER_PAID_EVENT
    )

    assert result is publish_result

    adapter.publish.assert_awaited_once_with(
        destination=destination, event=ORDER_PAID_EVENT
    )


@pytest.mark.asyncio
async def test_publish_raw_normalises_destination() -> None:
    adapter = create_autospec(BaseAsyncAdapter)
    publish_result = MagicMock()
    adapter.publish = AsyncMock(return_value=publish_result)

    event_stream = AsyncEventStream(adapter=adapter, event_source="my-service")

    destination = Destination(topic="shop", sub_topic="order")

    result = await event_stream.publish_raw(
        destination="shop.order", event=ORDER_PAID_EVENT
    )

    assert result is publish_result

    adapter.publish.assert_awaited_once_with(
        destination=destination, event=ORDER_PAID_EVENT
    )


@pytest.mark.asyncio
async def test_listen_with_single_subscriber_handles_a_single_message() -> None:
    adapter = create_autospec(BaseAsyncAdapter)
    adapter.listen = AsyncMock()
    adapter.ack = AsyncMock()

    event_stream = AsyncEventStream(adapter=adapter, event_source="my-service")

    subscribe_callback = AsyncMock()
    listener = event_stream.subscribe("shop.*")(subscribe_callback)

    await event_stream.listen()

    adapter.listen.assert_awaited_once()
    await_args_call = adapter.listen.await_args_list[0]
    async_listeners = cast(List[AsyncListener], await_args_call[0][0])

    async_listener = async_listeners[0]
    assert async_listener.listen_expression == ListenExpression(
        destination=Destination(topic="shop")
    )
    assert async_listener.event_body_type == PydanticEventBody

    await listener.callback(ORDER_PAID_INBOUND_MESSAGE)

    subscribe_callback.assert_awaited_once_with(ORDER_PAID_INBOUND_MESSAGE.event)

    adapter.ack.assert_awaited_once_with(ORDER_PAID_INBOUND_MESSAGE)


@pytest.mark.asyncio
async def test_listen_with_multiple_subscribers_handles_multiple_messages() -> None:
    adapter = create_autospec(BaseAsyncAdapter)
    adapter.listen = AsyncMock()
    adapter.ack = AsyncMock()

    event_stream = AsyncEventStream(adapter=adapter, event_source="my-service")

    subscribe_callback1 = AsyncMock()
    listener1 = event_stream.subscribe("shop.*")(subscribe_callback1)

    subscribe_callback2 = AsyncMock()
    listener2 = event_stream.subscribe("merchant_profile.business::fifo")(
        subscribe_callback2
    )

    await event_stream.listen()

    adapter.listen.assert_awaited_once()
    await_args_call = adapter.listen.await_args_list[0]
    async_listeners = cast(List[AsyncListener], await_args_call[0][0])

    async_listener1 = async_listeners[0]
    assert async_listener1.listen_expression == ListenExpression(
        destination=Destination(topic="shop")
    )
    assert async_listener1.event_body_type == PydanticEventBody

    async_listener2 = async_listeners[1]
    assert async_listener2.listen_expression == ListenExpression(
        destination=Destination(
            topic="merchant_profile", sub_topic="business", is_fifo=True
        )
    )
    assert async_listener2.event_body_type == PydanticEventBody

    await listener1.callback(ORDER_PAID_INBOUND_MESSAGE)
    await listener1.callback(ORDER_PLACED_INBOUND_MESSAGE)

    subscribe_callback1.assert_has_awaits(
        [
            call(ORDER_PAID_INBOUND_MESSAGE.event),
            call(ORDER_PLACED_INBOUND_MESSAGE.event),
        ]
    )
    assert subscribe_callback1.await_count == 2

    await listener2.callback(BUSINESS_REGISTERED_INBOUND_MESSAGE)

    subscribe_callback2.assert_awaited_once_with(
        BUSINESS_REGISTERED_INBOUND_MESSAGE.event
    )

    adapter.ack.assert_has_awaits(
        [
            call(ORDER_PAID_INBOUND_MESSAGE),
            call(ORDER_PLACED_INBOUND_MESSAGE),
            call(BUSINESS_REGISTERED_INBOUND_MESSAGE),
        ]
    )
    assert adapter.ack.await_count == 3


@pytest.mark.asyncio
async def test_listen_does_not_propagate_event_when_listen_expression_does_not_match() -> None:
    adapter = create_autospec(BaseAsyncAdapter)
    adapter.listen = AsyncMock()
    adapter.ack = AsyncMock()

    event_stream = AsyncEventStream(adapter=adapter, event_source="my-service")

    subscribe_callback = AsyncMock()
    listener = event_stream.subscribe("merchant_profile.*")(subscribe_callback)

    await event_stream.listen()

    adapter.listen.assert_awaited_once()
    await_args_call = adapter.listen.await_args_list[0]
    async_listeners = cast(List[AsyncListener], await_args_call[0][0])

    async_listener = async_listeners[0]
    assert async_listener.listen_expression == ListenExpression(
        destination=Destination(topic="merchant_profile")
    )
    assert async_listener.event_body_type == PydanticEventBody

    await listener.callback(ORDER_PAID_INBOUND_MESSAGE)

    subscribe_callback.assert_not_awaited()

    adapter.ack.assert_awaited_once_with(ORDER_PAID_INBOUND_MESSAGE)


@pytest.mark.asyncio
async def test_listen_handles_exceptions_gracefully_when_subscribe_callback_raises_an_exception() -> None:
    adapter = create_autospec(BaseAsyncAdapter)
    adapter.listen = AsyncMock()
    adapter.nack = AsyncMock()

    event_stream = AsyncEventStream(adapter=adapter, event_source="my-service")

    subscribe_callback = AsyncMock(
        side_effect=Exception("This exception is thrown by the subscriber callback.")
    )
    listener = event_stream.subscribe("shop.*")(subscribe_callback)

    await event_stream.listen()

    adapter.listen.assert_awaited_once()
    await_args_call = adapter.listen.await_args_list[0]
    async_listeners = cast(List[AsyncListener], await_args_call[0][0])

    async_listener = async_listeners[0]
    assert async_listener.listen_expression == ListenExpression(
        destination=Destination(topic="shop")
    )
    assert async_listener.event_body_type == PydanticEventBody

    await listener.callback(ORDER_PAID_INBOUND_MESSAGE)

    subscribe_callback.assert_awaited_once_with(ORDER_PAID_INBOUND_MESSAGE.event)

    adapter.nack.assert_awaited_once_with(ORDER_PAID_INBOUND_MESSAGE)


@pytest.mark.asyncio
async def test_listen_passes_exception_to_exception_handler_when_subscribe_callback_raises_an_exception() -> None:
    adapter = create_autospec(BaseAsyncAdapter)
    adapter.listen = AsyncMock()
    adapter.nack = AsyncMock()

    event_stream = AsyncEventStream(adapter=adapter, event_source="my-service")

    e = Exception("This exception is thrown by the event callback.")

    exception_handler = AsyncMock()

    subscribe_callback = AsyncMock(side_effect=e)
    listener = event_stream.subscribe("shop.*", exception_handler=exception_handler)(
        subscribe_callback
    )

    await event_stream.listen()

    adapter.listen.assert_awaited_once()
    await_args_call = adapter.listen.await_args_list[0]
    async_listeners = cast(List[AsyncListener], await_args_call[0][0])

    async_listener = async_listeners[0]
    assert async_listener.listen_expression == ListenExpression(
        destination=Destination(topic="shop")
    )
    assert async_listener.event_body_type == PydanticEventBody

    await listener.callback(ORDER_PAID_INBOUND_MESSAGE)

    subscribe_callback.assert_awaited_once_with(ORDER_PAID_INBOUND_MESSAGE.event)

    adapter.nack.assert_awaited_once_with(ORDER_PAID_INBOUND_MESSAGE)

    exception_handler.assert_awaited_once_with(e)


@pytest.mark.asyncio
async def test_listen_raises_an_exception_when_not_in_state_to_listen() -> None:
    async def listen(*args: Any, **kwargs: Any) -> None:
        await asyncio.sleep(0.15)

    adapter = create_autospec(BaseAsyncAdapter)
    adapter.listen = listen
    event_stream = AsyncEventStream(adapter=adapter, event_source="my-service")

    subscribe_callback = AsyncMock()
    event_stream.subscribe("shop.*")(subscribe_callback)

    listen_task = asyncio.create_task(event_stream.listen())
    await asyncio.sleep(0.1)

    assert event_stream.listener_state == ListenerState.RUNNING

    with pytest.raises(Exception) as excinfo:
        await event_stream.listen()

    assert (
        str(excinfo.value)
        == "The AsyncEventStream cannot be started given the current state."
    )

    # We await our sleepy listen task to avoid a warning about the task pending when the test completes.
    await listen_task


@pytest.mark.asyncio
async def test_shutdown_waits_for_pending_publish_tasks_to_complete() -> None:
    adapter = create_autospec(BaseAsyncAdapter)

    publish_result1 = MagicMock()
    publish_result2 = MagicMock()

    publish_call_count = 0

    async def publish_side_effect(destination: Destination, event: Event) -> Any:
        nonlocal publish_call_count

        publish_call_count += 1

        if publish_call_count == 1:
            return await asyncio.sleep(0.3, result=publish_result1)
        else:
            return publish_result2

    adapter.publish = AsyncMock(side_effect=publish_side_effect)
    adapter.close = AsyncMock()

    event_stream = AsyncEventStream(adapter=adapter, event_source="my-service")

    # The first publish() should finish after we call shutdown()
    task1: Task = asyncio.create_task(
        event_stream.publish(
            destination=Destination(topic="shop", sub_topic="order"),
            event_body=ORDER_PAID_EVENT_BODY,
        )
    )

    # The second publish() should finish immediately
    task2: Task = asyncio.create_task(
        event_stream.publish(
            destination=Destination(topic="shop", sub_topic="order"),
            event_body=ORDER_PLACED_EVENT_BODY,
        )
    )

    await asyncio.sleep(0.1)

    await event_stream.close()

    adapter.publish.assert_has_awaits(
        [
            call(
                destination=Destination(topic="shop", sub_topic="order"),
                event=ORDER_PAID_EVENT,
            ),
            call(
                destination=Destination(topic="shop", sub_topic="order"),
                event=ORDER_PLACED_EVENT,
            ),
        ]
    )
    assert adapter.publish.await_count == 2

    adapter.close.assert_awaited_once()

    assert task1.done()
    assert task1.result() == publish_result1

    assert task2.done()
    assert task2.result() == publish_result2


@pytest.mark.asyncio
async def test_shutdown_waits_for_pending_subscriber_tasks_to_complete() -> None:
    adapter = create_autospec(BaseAsyncAdapter)
    adapter.listen = AsyncMock()

    ack_call_count = 0

    ack_result1 = MagicMock()
    ack_result2 = MagicMock()
    ack_result3 = MagicMock()

    async def ack_side_effect(inbound_message: InboundMessage) -> Any:
        nonlocal ack_call_count

        ack_call_count += 1

        if ack_call_count == 1:
            return await asyncio.sleep(0.3, result=ack_result1)
        elif ack_call_count == 2:
            return await asyncio.sleep(0.25, result=ack_result2)
        elif ack_call_count == 3:
            return ack_result3

    adapter.ack = AsyncMock(side_effect=ack_side_effect)
    adapter.close = AsyncMock()

    event_stream = AsyncEventStream(adapter=adapter, event_source="my-service")

    subscribe_callback1_call_count = 0

    async def subscribe_callback1_side_effect(event: Event) -> Any:
        nonlocal subscribe_callback1_call_count

        subscribe_callback1_call_count += 1

        if subscribe_callback1_call_count == 1:
            return await asyncio.sleep(0.2)
        else:
            return await asyncio.sleep(0.15)

    subscribe_callback1 = AsyncMock(side_effect=subscribe_callback1_side_effect)
    listener1 = event_stream.subscribe("shop.*")(subscribe_callback1)

    subscribe_callback2 = AsyncMock()
    listener2 = event_stream.subscribe("merchant_profile.business::fifo")(
        subscribe_callback2
    )

    await event_stream.listen()

    adapter.listen.assert_awaited_once()
    await_args_call = adapter.listen.await_args_list[0]
    async_listeners = cast(List[AsyncListener], await_args_call[0][0])

    async_listener1 = async_listeners[0]
    assert async_listener1.listen_expression == ListenExpression(
        destination=Destination(topic="shop")
    )
    assert async_listener1.event_body_type == PydanticEventBody

    async_listener2 = async_listeners[1]
    assert async_listener2.listen_expression == ListenExpression(
        destination=Destination(
            topic="merchant_profile", sub_topic="business", is_fifo=True
        )
    )
    assert async_listener2.event_body_type == PydanticEventBody

    # The 1st message should finish processing after 0.2s
    task1: Task = asyncio.create_task(listener1.callback(ORDER_PAID_INBOUND_MESSAGE))  # type: ignore[arg-type]

    # The 2nd message should finish processing after 0.15s
    task2: Task = asyncio.create_task(listener1.callback(ORDER_PLACED_INBOUND_MESSAGE))  # type: ignore[arg-type]

    # The 3rd message should finish processing immediately.
    task3: Task = asyncio.create_task(listener2.callback(BUSINESS_REGISTERED_INBOUND_MESSAGE))  # type: ignore[arg-type]

    await asyncio.sleep(0.1)

    await event_stream.close()

    subscribe_callback1.assert_has_awaits(
        [
            call(ORDER_PAID_INBOUND_MESSAGE.event),
            call(ORDER_PLACED_INBOUND_MESSAGE.event),
        ]
    )
    assert subscribe_callback1.await_count == 2

    subscribe_callback2.assert_awaited_once_with(
        BUSINESS_REGISTERED_INBOUND_MESSAGE.event
    )

    adapter.ack.assert_has_awaits(
        [
            call(BUSINESS_REGISTERED_INBOUND_MESSAGE),
            call(ORDER_PLACED_INBOUND_MESSAGE),
            call(ORDER_PAID_INBOUND_MESSAGE),
        ]
    )
    assert adapter.ack.await_count == 3

    adapter.close.assert_awaited_once()

    assert task1.done()
    assert task2.done()
    assert task3.done()


@pytest.mark.asyncio
async def test_shutdown_when_listener_is_not_started() -> None:
    adapter = create_autospec(BaseAsyncAdapter)
    event_stream = AsyncEventStream(adapter=adapter, event_source="kyc")
    await event_stream.close()
    await event_stream.close()
    await event_stream.close()
    await event_stream.close()
