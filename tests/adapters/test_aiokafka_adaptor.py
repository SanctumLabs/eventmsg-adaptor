import asyncio
from typing import Any, Dict, List
from unittest import mock
from unittest.mock import ANY, AsyncMock, MagicMock, Mock, call

import pytest
from aiokafka import ConsumerRecord
from kafka import TopicPartition
from sanctumlabs.messageschema.messages.notifications.email.v1.events_pb2 import EmailV1

from tests import (
    ORDER_PAID_CONSUMER_RECORD,
    ORDER_PAID_EVENT,
    ORDER_PAID_KAFKA_INBOUND_MESSAGE,
    EMAIL_SENT_CONSUMER_RECORD,
    EMAIL_SENT_CONSUMER_RECORD_WITH_SCHEMA_REGISTRY_FRAMING,
    EMAIL_SENT_EVENT,
    EMAIL_SENT_INBOUND_MESSAGE,
    EMAIL_SENT_INBOUND_MESSAGE_WITH_SCHEMA_REGISTRY_FRAMING,
    EMAIL_SENT_KAFKA_HEADERS,
    OrderPaid,
)
from eventmsg_adaptor.adapters.kafka.aiokafka import (
    AIOKafkaAdapter,
    ConsumerState,
    DefaultRebalancer,
    key_serializer,
)
from eventmsg_adaptor.serializers.confluent_protobuf_serializer import ConfluentProtobufSerializer
from eventmsg_adaptor.serializers.protobuf_serializer import ProtobufSerializer
from eventmsg_adaptor.serializers.pydantic_serializer import PydanticSerializer
from eventmsg_adaptor.schema import AsyncListener, Destination, ListenExpression


def test_key_serializer_encodes_a_string_as_bytes() -> None:
    key_as_bytes = key_serializer("correlation_id")

    assert key_as_bytes == b"correlation_id"


def test_key_serializer_returns_empty_byte_string_if_value_is_empty_or_none() -> None:
    assert key_serializer("") == b""
    assert key_serializer(None) == b""


@pytest.mark.asyncio
@mock.patch("eventmsg_adaptor.adapters.kafka.aiokafka.AIOKafkaProducer", autospec=True)
async def test_publish_is_able_to_produce_a_protobuf_type_message(
    mock_producer_constructor: Mock,
) -> None:
    mock_producer = mock_producer_constructor.return_value

    mock_producer.start = AsyncMock()
    mock_producer.send_and_wait = AsyncMock(return_value="foobar")
    mock_producer.stop = AsyncMock()

    adapter = AIOKafkaAdapter(
        bootstrap_servers=["localhost:9092"],
        group_id="niosys",
        serializer=ProtobufSerializer(),
    )

    publish_result = await adapter.publish(
        Destination(topic="email_sent_v1"), EMAIL_SENT_EVENT
    )

    assert publish_result == "foobar"

    mock_producer_constructor.assert_called_once_with(
        bootstrap_servers=["localhost:9092"],
        sasl_plain_username=None,
        sasl_plain_password=None,
        security_protocol="PLAINTEXT",
        ssl_context=ANY,
        enable_idempotence=True,
        key_serializer=key_serializer,
        value_serializer=ANY,
        request_timeout_ms=10000,
    )

    mock_producer.start.assert_awaited_once()

    mock_producer.send_and_wait.assert_awaited_once_with(
        topic="email_sent_v1",
        value=EMAIL_SENT_EVENT.body,
        key=None,
        headers=EMAIL_SENT_KAFKA_HEADERS,
    )

    mock_producer.stop.assert_awaited_once()


@pytest.mark.asyncio
@mock.patch("eventmsg_adaptor.adapters.kafka.aiokafka.AIOKafkaProducer", autospec=True)
async def test_publish_is_able_to_produce_a_pydantic_event_body_type_message(
    mock_producer_constructor: Mock,
) -> None:
    mock_producer = mock_producer_constructor.return_value

    mock_producer.start = AsyncMock()
    mock_producer.send_and_wait = AsyncMock(return_value="foobar")
    mock_producer.stop = AsyncMock()

    adapter = AIOKafkaAdapter(
        bootstrap_servers=["localhost:9092"],
        group_id="niosys",
        serializer=ProtobufSerializer(),
    )

    publish_result = await adapter.publish(Destination(topic="shop"), ORDER_PAID_EVENT)

    assert publish_result == "foobar"

    mock_producer_constructor.assert_called_once_with(
        bootstrap_servers=["localhost:9092"],
        sasl_plain_username=None,
        sasl_plain_password=None,
        security_protocol="PLAINTEXT",
        ssl_context=ANY,
        enable_idempotence=True,
        key_serializer=key_serializer,
        value_serializer=ANY,
        request_timeout_ms=10000,
    )

    mock_producer.start.assert_awaited_once()

    mock_producer.send_and_wait.assert_awaited_once_with(
        topic="shop",
        value=ORDER_PAID_EVENT.body,
        key=None,
        headers=[
            ("id", b"fae9ef15-06f0-46b9-95f1-2a75dd482687"),
            ("event_name", b"order_paid"),
            ("destination", b"shop"),
            ("version", b"1.0"),
            ("timestamp", b"2020-09-08T19:53:46"),
            ("source", b"my-service"),
            ("correlation_id", b""),
            ("group_id", b""),
        ],
    )

    mock_producer.stop.assert_awaited_once()


@pytest.mark.asyncio
@mock.patch("eventmsg_adaptor.adapters.kafka.aiokafka.AIOKafkaProducer", autospec=True)
async def test_publish_uses_sasl_auth_with_producer_when_configured(
    mock_producer_constructor: Mock,
) -> None:
    mock_producer = mock_producer_constructor.return_value

    mock_producer.start = AsyncMock()
    mock_producer.send_and_wait = AsyncMock(return_value="foobar")
    mock_producer.stop = AsyncMock()

    adapter = AIOKafkaAdapter(
        bootstrap_servers=["localhost:9092"],
        group_id="niosys",
        sasl_username="SanctumLabs@",
        sasl_password="VerySecretP@ssword",
        serializer=ProtobufSerializer(),
    )

    publish_result = await adapter.publish(
        Destination(topic="email_sent_v1"), EMAIL_SENT_EVENT
    )

    assert publish_result == "foobar"

    mock_producer_constructor.assert_called_once_with(
        bootstrap_servers=["localhost:9092"],
        sasl_plain_username="SanctumLabs@",
        sasl_plain_password="VerySecretP@ssword",
        security_protocol="SASL_SSL",
        ssl_context=ANY,
        enable_idempotence=True,
        key_serializer=key_serializer,
        value_serializer=ANY,
        request_timeout_ms=10000,
    )

    mock_producer.start.assert_awaited_once()

    mock_producer.send_and_wait.assert_awaited_once_with(
        topic="email_sent_v1",
        value=EMAIL_SENT_EVENT.body,
        key=None,
        headers=EMAIL_SENT_KAFKA_HEADERS,
    )

    mock_producer.stop.assert_awaited_once()


@pytest.mark.asyncio
@mock.patch("eventmsg_adaptor.adapters.kafka.aiokafka.AIOKafkaProducer", autospec=True)
async def test_publish_always_safely_stops_producer_even_if_publish_fails(
    mock_producer_constructor: Mock,
) -> None:
    mock_producer = mock_producer_constructor.return_value

    mock_producer.start = AsyncMock()
    mock_producer.send_and_wait = AsyncMock(
        side_effect=Exception("The producer failed to send the message.")
    )
    mock_producer.stop = AsyncMock()

    adapter = AIOKafkaAdapter(
        bootstrap_servers=["localhost:9092"],
        group_id="niosys",
        serializer=ProtobufSerializer(),
    )

    with pytest.raises(Exception):
        await adapter.publish(
            Destination(topic="email_sent_v1"), EMAIL_SENT_EVENT
        )

    mock_producer_constructor.assert_called_once_with(
        bootstrap_servers=["localhost:9092"],
        sasl_plain_username=None,
        sasl_plain_password=None,
        security_protocol="PLAINTEXT",
        ssl_context=ANY,
        enable_idempotence=True,
        key_serializer=key_serializer,
        value_serializer=ANY,
        request_timeout_ms=10000,
    )

    mock_producer.start.assert_awaited_once()

    mock_producer.send_and_wait.assert_awaited_once_with(
        topic="email_sent_v1",
        value=EMAIL_SENT_EVENT.body,
        key=None,
        headers=EMAIL_SENT_KAFKA_HEADERS,
    )

    mock_producer.stop.assert_awaited_once()


@pytest.mark.asyncio
@mock.patch("eventmsg_adaptor.adapters.kafka.aiokafka.AIOKafkaConsumer", autospec=True)
async def test_listen_handles_subscribing_consumer_to_a_single_topic(
    mock_consumer_constructor: Mock,
) -> None:
    adapter = AIOKafkaAdapter(
        bootstrap_servers=["localhost:9092"],
        group_id="niosys",
        serializer=ProtobufSerializer(),
    )

    mock_consumer = mock_consumer_constructor.return_value

    mock_consumer.subscribe = MagicMock()
    mock_consumer.start = AsyncMock()
    mock_consumer.stop = AsyncMock()

    async def getmany_side_effect(
        *args: Any, **kwargs: Any
    ) -> Dict[TopicPartition, List[ConsumerRecord]]:
        assert adapter.consumer_state == ConsumerState.RUNNING

        await adapter.close()

        return {}

    mock_consumer.getmany = AsyncMock(side_effect=getmany_side_effect)

    assert adapter.consumer_state == ConsumerState.STOPPED

    await adapter.listen(
        [
            AsyncListener(
                listen_expression=ListenExpression(
                    destination=Destination(topic="email_sent_v1")
                ),
                callback=AsyncMock(),
                event_body_type=EmailV1,
            )
        ]
    )

    mock_consumer_constructor.assert_called_once_with(
        group_id="niosys",
        bootstrap_servers=["localhost:9092"],
        sasl_plain_username=None,
        sasl_plain_password=None,
        security_protocol="PLAINTEXT",
        ssl_context=ANY,
        enable_auto_commit=False,
    )

    mock_consumer.subscribe.assert_called_once_with(
        ["email_sent_v1"], listener=ANY
    )

    mock_consumer.start.assert_awaited_once()

    mock_consumer.getmany.assert_awaited_once_with(timeout_ms=1000)

    mock_consumer.stop.assert_awaited_once()
    assert adapter.consumer_state == ConsumerState.STOPPED


@pytest.mark.asyncio
@mock.patch("eventmsg_adaptor.adapters.kafka.aiokafka.AIOKafkaConsumer", autospec=True)
async def test_listen_handles_subscribing_consumer_to_multiple_topics(
    mock_consumer_constructor: Mock,
) -> None:
    adapter = AIOKafkaAdapter(
        bootstrap_servers=["localhost:9092"],
        group_id="niosys",
        serializer=ProtobufSerializer(),
    )

    mock_consumer = mock_consumer_constructor.return_value

    mock_consumer.subscribe = MagicMock()
    mock_consumer.start = AsyncMock()
    mock_consumer.stop = AsyncMock()

    async def getmany_side_effect(
        *args: Any, **kwargs: Any
    ) -> Dict[TopicPartition, List[ConsumerRecord]]:
        assert adapter.consumer_state == ConsumerState.RUNNING

        await adapter.close()

        return {}

    mock_consumer.getmany = AsyncMock(side_effect=getmany_side_effect)

    assert adapter.consumer_state == ConsumerState.STOPPED

    await adapter.listen(
        [
            AsyncListener(
                listen_expression=ListenExpression(
                    destination=Destination(topic="email_sent_v1")
                ),
                callback=AsyncMock(),
                event_body_type=EmailV1,
            ),
            AsyncListener(
                listen_expression=ListenExpression(
                    destination=Destination(topic="shop")
                ),
                callback=AsyncMock(),
                event_body_type=OrderPaid,
            ),
        ]
    )

    mock_consumer_constructor.assert_called_once_with(
        group_id="niosys",
        bootstrap_servers=["localhost:9092"],
        sasl_plain_username=None,
        sasl_plain_password=None,
        security_protocol="PLAINTEXT",
        ssl_context=ANY,
        enable_auto_commit=False,
    )

    mock_consumer.subscribe.assert_called_once_with(
        ["email_sent_v1", "shop"],
        listener=ANY,
    )

    mock_consumer.start.assert_awaited_once()

    mock_consumer.getmany.assert_awaited_once_with(timeout_ms=1000)

    mock_consumer.stop.assert_awaited_once()
    assert adapter.consumer_state == ConsumerState.STOPPED


@pytest.mark.asyncio
@mock.patch("eventmsg_adaptor.adapters.kafka.aiokafka.AIOKafkaConsumer", autospec=True)
async def test_listen_is_able_to_receive_a_protobuf_type_message(
    mock_consumer_constructor: Mock,
) -> None:
    adapter = AIOKafkaAdapter(
        bootstrap_servers=["localhost:9092"],
        group_id="niosys",
        serializer=ProtobufSerializer(),
    )

    mock_consumer = mock_consumer_constructor.return_value

    mock_consumer.subscribe = MagicMock()
    mock_consumer.start = AsyncMock()
    mock_consumer.stop = AsyncMock()

    async def getmany_side_effect(
        *args: Any, **kwargs: Any
    ) -> Dict[TopicPartition, List[ConsumerRecord]]:
        assert adapter.consumer_state == ConsumerState.RUNNING

        await adapter.close()

        return {
            TopicPartition("email_sent_v1", 0): [
                EMAIL_SENT_CONSUMER_RECORD
            ]
        }

    mock_consumer.getmany = AsyncMock(side_effect=getmany_side_effect)

    assert adapter.consumer_state == ConsumerState.STOPPED

    listener_callback = AsyncMock()

    await adapter.listen(
        [
            AsyncListener(
                listen_expression=ListenExpression(
                    destination=Destination(topic="email_sent_v1")
                ),
                callback=listener_callback,
                event_body_type=EmailV1,
            )
        ]
    )

    mock_consumer_constructor.assert_called_once_with(
        group_id="niosys",
        bootstrap_servers=["localhost:9092"],
        sasl_plain_username=None,
        sasl_plain_password=None,
        security_protocol="PLAINTEXT",
        ssl_context=ANY,
        enable_auto_commit=False,
    )

    mock_consumer.subscribe.assert_called_once_with(
        ["email_sent_v1"],
        listener=ANY,
    )

    mock_consumer.start.assert_awaited_once()

    mock_consumer.getmany.assert_awaited_once_with(timeout_ms=1000)

    listener_callback.assert_awaited_once_with(EMAIL_SENT_INBOUND_MESSAGE)

    mock_consumer.stop.assert_awaited_once()
    assert adapter.consumer_state == ConsumerState.STOPPED


@pytest.mark.asyncio
@mock.patch("eventmsg_adaptor.adapters.kafka.aiokafka.AIOKafkaConsumer", autospec=True)
async def test_listen_is_able_to_receive_a_pydantic_event_body_type_message(
    mock_consumer_constructor: Mock,
) -> None:
    adapter = AIOKafkaAdapter(
        bootstrap_servers=["localhost:9092"],
        group_id="niosys",
        serializer=PydanticSerializer(),
    )

    mock_consumer = mock_consumer_constructor.return_value

    mock_consumer.subscribe = MagicMock()
    mock_consumer.start = AsyncMock()
    mock_consumer.stop = AsyncMock()

    async def getmany_side_effect(
        *args: Any, **kwargs: Any
    ) -> Dict[TopicPartition, List[ConsumerRecord]]:
        assert adapter.consumer_state == ConsumerState.RUNNING

        await adapter.close()

        return {TopicPartition("shop", 0): [ORDER_PAID_CONSUMER_RECORD]}

    mock_consumer.getmany = AsyncMock(side_effect=getmany_side_effect)

    assert adapter.consumer_state == ConsumerState.STOPPED

    listener_callback = AsyncMock()

    await adapter.listen(
        [
            AsyncListener(
                listen_expression=ListenExpression(
                    destination=Destination(topic="shop")
                ),
                callback=listener_callback,
                event_body_type=OrderPaid,
            )
        ]
    )

    mock_consumer_constructor.assert_called_once_with(
        group_id="niosys",
        bootstrap_servers=["localhost:9092"],
        sasl_plain_username=None,
        sasl_plain_password=None,
        security_protocol="PLAINTEXT",
        ssl_context=ANY,
        enable_auto_commit=False,
    )

    mock_consumer.subscribe.assert_called_once_with(
        ["shop"],
        listener=ANY,
    )

    mock_consumer.start.assert_awaited_once()

    mock_consumer.getmany.assert_awaited_once_with(timeout_ms=1000)

    listener_callback.assert_awaited()

    listener_callback.assert_awaited_once_with(ORDER_PAID_KAFKA_INBOUND_MESSAGE)

    mock_consumer.stop.assert_awaited_once()
    assert adapter.consumer_state == ConsumerState.STOPPED


@pytest.mark.asyncio
@mock.patch("eventmsg_adaptor.adapters.kafka.aiokafka.AIOKafkaConsumer", autospec=True)
async def test_listen_gracefully_handles_a_message_which_cannot_be_parsed(
    mock_consumer_constructor: Mock,
) -> None:
    adapter = AIOKafkaAdapter(
        bootstrap_servers=["localhost:9092"],
        group_id="niosys",
        serializer=ProtobufSerializer(),
    )

    mock_consumer = mock_consumer_constructor.return_value

    mock_consumer.subscribe = MagicMock()
    mock_consumer.start = AsyncMock()
    mock_consumer.stop = AsyncMock()

    async def getmany_side_effect(
        *args: Any, **kwargs: Any
    ) -> Dict[TopicPartition, List[ConsumerRecord]]:
        assert adapter.consumer_state == ConsumerState.RUNNING

        await adapter.close()

        return {
            TopicPartition("email_sent_v1", 0): [
                ConsumerRecord(
                    topic="email_sent_v1",
                    partition=0,
                    offset=0,
                    timestamp=1688141486475,
                    timestamp_type=0,
                    key=None,
                    value=b"this is a bogus message",
                    checksum=0,
                    serialized_key_size=1,
                    serialized_value_size=123,
                    headers={},
                )
            ]
        }

    mock_consumer.getmany = AsyncMock(side_effect=getmany_side_effect)

    assert adapter.consumer_state == ConsumerState.STOPPED

    listener_callback = AsyncMock()

    await adapter.listen(
        [
            AsyncListener(
                listen_expression=ListenExpression(
                    destination=Destination(topic="email_sent_v1")
                ),
                callback=listener_callback,
                event_body_type=EmailV1,
            )
        ]
    )

    mock_consumer_constructor.assert_called_once_with(
        group_id="niosys",
        bootstrap_servers=["localhost:9092"],
        sasl_plain_username=None,
        sasl_plain_password=None,
        security_protocol="PLAINTEXT",
        ssl_context=ANY,
        enable_auto_commit=False,
    )

    mock_consumer.subscribe.assert_called_once_with(
        ["email_sent_v1"],
        listener=ANY,
    )

    mock_consumer.start.assert_awaited_once()

    mock_consumer.getmany.assert_awaited_once_with(timeout_ms=1000)

    listener_callback.assert_not_awaited()

    mock_consumer.stop.assert_awaited_once()
    assert adapter.consumer_state == ConsumerState.STOPPED


@pytest.mark.asyncio
@mock.patch("eventmsg_adaptor.adapters.kafka.aiokafka.AIOKafkaConsumer", autospec=True)
async def test_listen_is_able_to_receive_a_batch_of_messages(
    mock_consumer_constructor: Mock,
) -> None:
    adapter = AIOKafkaAdapter(
        bootstrap_servers=["localhost:9092"],
        group_id="niosys",
        serializer=ProtobufSerializer(),
    )

    mock_consumer = mock_consumer_constructor.return_value

    mock_consumer.subscribe = MagicMock()
    mock_consumer.start = AsyncMock()
    mock_consumer.stop = AsyncMock()

    getmany_call_count = 0

    async def getmany_side_effect(
        *args: Any, **kwargs: Any
    ) -> Dict[TopicPartition, List[ConsumerRecord]]:
        nonlocal getmany_call_count

        getmany_call_count += 1

        assert adapter.consumer_state == ConsumerState.RUNNING

        if getmany_call_count == 1:
            return {
                TopicPartition("email_sent_v1", 0): [
                    EMAIL_SENT_CONSUMER_RECORD,
                    EMAIL_SENT_CONSUMER_RECORD,
                ],
                TopicPartition("email_sent_v1", 1): [
                    EMAIL_SENT_CONSUMER_RECORD
                ],
            }
        else:
            await adapter.close()

            return {
                TopicPartition("email_sent_v1", 0): [
                    EMAIL_SENT_CONSUMER_RECORD
                ],
            }

    mock_consumer.getmany = AsyncMock(side_effect=getmany_side_effect)

    assert adapter.consumer_state == ConsumerState.STOPPED

    listener_callback = AsyncMock()

    await adapter.listen(
        [
            AsyncListener(
                listen_expression=ListenExpression(
                    destination=Destination(topic="email_sent_v1")
                ),
                callback=listener_callback,
                event_body_type=EmailV1,
            ),
        ]
    )

    mock_consumer_constructor.assert_called_once_with(
        group_id="niosys",
        bootstrap_servers=["localhost:9092"],
        sasl_plain_username=None,
        sasl_plain_password=None,
        security_protocol="PLAINTEXT",
        ssl_context=ANY,
        enable_auto_commit=False,
    )

    mock_consumer.subscribe.assert_called_once_with(
        ["email_sent_v1"],
        listener=ANY,
    )

    mock_consumer.start.assert_awaited_once()

    assert mock_consumer.getmany.await_count == 2
    mock_consumer.getmany.assert_has_awaits(
        [call(timeout_ms=1000), call(timeout_ms=1000)]
    )

    assert listener_callback.await_count == 4
    listener_callback.assert_has_awaits(
        [
            call(EMAIL_SENT_INBOUND_MESSAGE),
            call(EMAIL_SENT_INBOUND_MESSAGE),
            call(EMAIL_SENT_INBOUND_MESSAGE),
        ]
    )

    mock_consumer.stop.assert_awaited_once()
    assert adapter.consumer_state == ConsumerState.STOPPED


@pytest.mark.asyncio
@mock.patch("eventmsg_adaptor.adapters.kafka.aiokafka.AIOKafkaConsumer", autospec=True)
async def test_listen_handles_messages_correctly_during_a_consumer_rebalance(
    mock_consumer_constructor: Mock,
) -> None:
    adapter = AIOKafkaAdapter(
        bootstrap_servers=["localhost:9092"],
        group_id="niosys",
        serializer=ProtobufSerializer(),
    )

    mock_consumer = mock_consumer_constructor.return_value

    mock_consumer.subscribe = MagicMock()
    mock_consumer.start = AsyncMock()
    mock_consumer.stop = AsyncMock()

    getmany_call_count = 0

    async def getmany_side_effect(
        *args: Any, **kwargs: Any
    ) -> Dict[TopicPartition, List[ConsumerRecord]]:
        nonlocal getmany_call_count

        getmany_call_count += 1

        if getmany_call_count == 1:
            # The first time we do this call, we'll return a single message.
            # We expect this message to be handled immediately.

            assert adapter.consumer_state == ConsumerState.RUNNING

            return {
                TopicPartition("email_sent_v1", 0): [
                    EMAIL_SENT_CONSUMER_RECORD
                ]
            }
        elif getmany_call_count == 2:
            # The 2nd time we do this call, we'll fake a re-balance and return a single message.
            # We expect this message to be ignored now since a re-balance is in progress.

            assert adapter.consumer_state == ConsumerState.RUNNING

            asyncio.create_task(
                adapter.rebalance_listener.on_partitions_revoked(
                    [TopicPartition("email_sent_v1", 0)]
                )
            )

            await asyncio.sleep(0.1)

            return {
                TopicPartition("email_sent_v1", 0): [
                    EMAIL_SENT_CONSUMER_RECORD
                ],
            }
        elif getmany_call_count == 3:
            # The 3rd time we do this call, we've finished our own re-balance handling and we need to pass control back
            # to the listener. We don't process any messages until we have our partition(s) assigned, so we'll fake
            # that here.

            assert adapter.consumer_state == ConsumerState.REBALANCING

            await adapter.rebalance_listener.on_partitions_assigned(
                [TopicPartition("email_sent_v1", 0)]
            )

            assert adapter.consumer_state == ConsumerState.RUNNING  # type: ignore[comparison-overlap]

            return {  # type: ignore[unreachable]
                TopicPartition("email_sent_v1", 0): [
                    EMAIL_SENT_CONSUMER_RECORD
                ],
            }
        else:
            assert adapter.consumer_state == ConsumerState.RUNNING

            await adapter.close()

            return {}

    mock_consumer.getmany = AsyncMock(side_effect=getmany_side_effect)

    assert adapter.consumer_state == ConsumerState.STOPPED

    listener_callback = AsyncMock()

    await adapter.listen(
        [
            AsyncListener(
                listen_expression=ListenExpression(
                    destination=Destination(topic="email_sent_v1")
                ),
                callback=listener_callback,
                event_body_type=EmailV1,
            ),
        ]
    )

    mock_consumer_constructor.assert_called_once_with(
        group_id="niosys",
        bootstrap_servers=["localhost:9092"],
        sasl_plain_username=None,
        sasl_plain_password=None,
        security_protocol="PLAINTEXT",
        ssl_context=ANY,
        enable_auto_commit=False,
    )

    mock_consumer.subscribe.assert_called_once_with(
        ["email_sent_v1"],
        listener=ANY,
    )

    mock_consumer.start.assert_awaited_once()

    assert mock_consumer.getmany.await_count == 4
    mock_consumer.getmany.assert_has_awaits(
        [
            call(timeout_ms=1000),
            call(timeout_ms=1000),
            call(timeout_ms=1000),
            call(timeout_ms=1000),
        ]
    )

    listener_callback.assert_has_awaits(
        [call(EMAIL_SENT_INBOUND_MESSAGE), call(EMAIL_SENT_INBOUND_MESSAGE)]
    )

    mock_consumer.stop.assert_awaited_once()
    assert adapter.consumer_state == ConsumerState.STOPPED


@pytest.mark.asyncio
@mock.patch("eventmsg_adaptor.adapters.kafka.aiokafka.AIOKafkaConsumer", autospec=True)
async def test_listen_closes_consumer_cleanly_when_adapter_is_closed(
    mock_consumer_constructor: Mock,
) -> None:
    adapter = AIOKafkaAdapter(
        bootstrap_servers=["localhost:9092"],
        group_id="niosys",
        serializer=ProtobufSerializer(),
    )

    mock_consumer = mock_consumer_constructor.return_value

    mock_consumer.subscribe = MagicMock()
    mock_consumer.start = AsyncMock()
    mock_consumer.stop = AsyncMock()

    async def getmany_side_effect(*args: Any, **kwargs: Any) -> None:
        assert adapter.consumer_state == ConsumerState.RUNNING

        raise Exception("This exception should cause the listener to stop.")

    mock_consumer.getmany = AsyncMock(side_effect=getmany_side_effect)

    assert adapter.consumer_state == ConsumerState.STOPPED

    with pytest.raises(Exception):
        await adapter.listen(
            [
                AsyncListener(
                    listen_expression=ListenExpression(
                        destination=Destination(topic="email_sent_v1")
                    ),
                    callback=AsyncMock(),
                    event_body_type=EmailV1,
                )
            ]
        )

    mock_consumer_constructor.assert_called_once_with(
        group_id="niosys",
        bootstrap_servers=["localhost:9092"],
        sasl_plain_username=None,
        sasl_plain_password=None,
        security_protocol="PLAINTEXT",
        ssl_context=ANY,
        enable_auto_commit=False,
    )

    mock_consumer.subscribe.assert_called_once_with(
        ["email_sent_v1"],
        listener=ANY,
    )

    mock_consumer.start.assert_awaited_once()

    mock_consumer.getmany.assert_awaited_once_with(timeout_ms=1000)

    mock_consumer.stop.assert_awaited_once()
    assert adapter.consumer_state == ConsumerState.STOPPED


@pytest.mark.asyncio
@mock.patch("eventmsg_adaptor.adapters.kafka.aiokafka.AIOKafkaConsumer", autospec=True)
async def test_listen_raises_an_exception_when_the_consumer_has_already_started(
    mock_consumer_constructor: Mock,
) -> None:
    adapter = AIOKafkaAdapter(
        bootstrap_servers=["localhost:9092"],
        group_id="niosys",
        serializer=ProtobufSerializer(),
    )

    mock_consumer = mock_consumer_constructor.return_value

    mock_consumer.subscribe = MagicMock()
    mock_consumer.start = AsyncMock()
    mock_consumer.stop = AsyncMock()

    async def getmany_side_effect(
        *args: Any, **kwargs: Any
    ) -> Dict[TopicPartition, List[ConsumerRecord]]:
        with pytest.raises(Exception, match="The consumer has already been started."):
            await adapter.listen(
                [
                    AsyncListener(
                        listen_expression=ListenExpression(
                            destination=Destination(topic="email_sent_v1")
                        ),
                        callback=AsyncMock(),
                        event_body_type=EmailV1,
                    )
                ]
            )

        await adapter.close()

        return {}

    mock_consumer.getmany = AsyncMock(side_effect=getmany_side_effect)

    await adapter.listen(
        [
            AsyncListener(
                listen_expression=ListenExpression(
                    destination=Destination(topic="email_sent_v1")
                ),
                callback=AsyncMock(),
                event_body_type=EmailV1,
            )
        ]
    )

    mock_consumer_constructor.assert_called_once_with(
        group_id="niosys",
        bootstrap_servers=["localhost:9092"],
        sasl_plain_username=None,
        sasl_plain_password=None,
        security_protocol="PLAINTEXT",
        ssl_context=ANY,
        enable_auto_commit=False,
    )

    mock_consumer.start.assert_awaited_once()

    mock_consumer.getmany.assert_awaited_once_with(timeout_ms=1000)

    mock_consumer.stop.assert_awaited_once()
    assert adapter.consumer_state == ConsumerState.STOPPED


@pytest.mark.asyncio
@mock.patch("eventmsg_adaptor.adapters.kafka.aiokafka.AIOKafkaConsumer", autospec=True)
async def test_listen_uses_sasl_auth_with_consumer_when_configured(
    mock_consumer_constructor: Mock,
) -> None:
    adapter = AIOKafkaAdapter(
        bootstrap_servers=["localhost:9092"],
        group_id="niosys",
        sasl_username="SanctumLabs@",
        sasl_password="VerySecretP@ssword",
        serializer=ProtobufSerializer(),
    )

    mock_consumer = mock_consumer_constructor.return_value

    mock_consumer.subscribe = MagicMock()
    mock_consumer.start = AsyncMock()
    mock_consumer.stop = AsyncMock()

    async def getmany_side_effect(
        *args: Any, **kwargs: Any
    ) -> Dict[TopicPartition, List[ConsumerRecord]]:
        assert adapter.consumer_state == ConsumerState.RUNNING

        await adapter.close()

        return {
            TopicPartition("email_sent_v1", 0): [
                EMAIL_SENT_CONSUMER_RECORD
            ]
        }

    mock_consumer.getmany = AsyncMock(side_effect=getmany_side_effect)

    assert adapter.consumer_state == ConsumerState.STOPPED

    listener_callback = AsyncMock()

    await adapter.listen(
        [
            AsyncListener(
                listen_expression=ListenExpression(
                    destination=Destination(topic="email_sent_v1")
                ),
                callback=listener_callback,
                event_body_type=EmailV1,
            )
        ]
    )

    mock_consumer_constructor.assert_called_once_with(
        group_id="niosys",
        bootstrap_servers=["localhost:9092"],
        sasl_plain_username="SanctumLabs@",
        sasl_plain_password="VerySecretP@ssword",
        security_protocol="SASL_SSL",
        ssl_context=ANY,
        enable_auto_commit=False,
    )

    mock_consumer.subscribe.assert_called_once_with(
        ["email_sent_v1"],
        listener=ANY,
    )

    mock_consumer.start.assert_awaited_once()

    mock_consumer.getmany.assert_awaited_once_with(timeout_ms=1000)

    listener_callback.assert_awaited_once_with(EMAIL_SENT_INBOUND_MESSAGE)

    mock_consumer.stop.assert_awaited_once()
    assert adapter.consumer_state == ConsumerState.STOPPED


@pytest.mark.asyncio
@mock.patch("eventmsg_adaptor.adapters.kafka.aiokafka.AIOKafkaConsumer", autospec=True)
async def test_listen_is_able_to_receive_a_protobuf_type_message_with_confluent_schema_registry_framing(
    mock_consumer_constructor: Mock,
) -> None:
    adapter = AIOKafkaAdapter(
        bootstrap_servers=["localhost:9092"],
        group_id="niosys",
        sasl_username="SanctumLabs@",
        sasl_password="VerySecretP@ssword",
        serializer=ConfluentProtobufSerializer(),
    )

    mock_consumer = mock_consumer_constructor.return_value

    mock_consumer.subscribe = MagicMock()
    mock_consumer.start = AsyncMock()
    mock_consumer.stop = AsyncMock()

    async def getmany_side_effect(
        *args: Any, **kwargs: Any
    ) -> Dict[TopicPartition, List[ConsumerRecord]]:
        assert adapter.consumer_state == ConsumerState.RUNNING

        await adapter.close()

        return {
            TopicPartition("email_sent_v1", 0): [
                EMAIL_SENT_CONSUMER_RECORD_WITH_SCHEMA_REGISTRY_FRAMING
            ]
        }

    mock_consumer.getmany = AsyncMock(side_effect=getmany_side_effect)

    assert adapter.consumer_state == ConsumerState.STOPPED

    listener_callback = AsyncMock()

    await adapter.listen(
        [
            AsyncListener(
                listen_expression=ListenExpression(
                    destination=Destination(topic="email_sent_v1")
                ),
                callback=listener_callback,
                event_body_type=EmailV1,
            )
        ]
    )

    mock_consumer_constructor.assert_called_once_with(
        group_id="niosys",
        bootstrap_servers=["localhost:9092"],
        sasl_plain_username="SanctumLabs@",
        sasl_plain_password="VerySecretP@ssword",
        security_protocol="SASL_SSL",
        ssl_context=ANY,
        enable_auto_commit=False,
    )

    mock_consumer.subscribe.assert_called_once_with(
        ["email_sent_v1"],
        listener=ANY,
    )

    mock_consumer.start.assert_awaited_once()

    mock_consumer.getmany.assert_awaited_once_with(timeout_ms=1000)

    listener_callback.assert_awaited_once_with(
        EMAIL_SENT_INBOUND_MESSAGE_WITH_SCHEMA_REGISTRY_FRAMING
    )

    mock_consumer.stop.assert_awaited_once()
    assert adapter.consumer_state == ConsumerState.STOPPED


@pytest.mark.asyncio
@mock.patch("eventmsg_adaptor.adapters.kafka.aiokafka.AIOKafkaConsumer", autospec=True)
async def test_ack_commits_offset(
    mock_consumer_constructor: Mock,
) -> None:
    adapter = AIOKafkaAdapter(
        bootstrap_servers=["localhost:9092"],
        group_id="niosys",
        serializer=ProtobufSerializer(),
    )

    mock_consumer = mock_consumer_constructor.return_value

    mock_consumer.commit = AsyncMock()

    await adapter.ack(EMAIL_SENT_INBOUND_MESSAGE)

    mock_consumer_constructor.assert_called_once_with(
        group_id="niosys",
        bootstrap_servers=["localhost:9092"],
        sasl_plain_username=None,
        sasl_plain_password=None,
        security_protocol="PLAINTEXT",
        ssl_context=ANY,
        enable_auto_commit=False,
    )

    mock_consumer.commit.assert_awaited_once_with(
        {TopicPartition("email_sent_v1", 0): 5}
    )


@pytest.mark.asyncio
async def test_nack_does_nothing() -> None:
    adapter = AIOKafkaAdapter(
        bootstrap_servers=["localhost:9092"],
        group_id="niosys",
        serializer=ProtobufSerializer(),
    )

    await adapter.nack(EMAIL_SENT_INBOUND_MESSAGE)


@pytest.mark.asyncio
async def test_default_rebalancer_does_not_call_user_on_partitions_revoked_callback_when_nothing_is_revoked() -> None:
    on_partitions_revoked_callback = AsyncMock()
    on_partitions_assigned_callback = AsyncMock()

    rebalancer = DefaultRebalancer(
        on_partitions_revoked_callback=on_partitions_revoked_callback,
        on_partitions_assigned_callback=on_partitions_assigned_callback,
    )

    await rebalancer.on_partitions_revoked([])

    on_partitions_revoked_callback.assert_not_awaited()


@pytest.mark.asyncio
async def test_default_rebalancer_calls_user_on_partitions_revoked_callback_and_waits_for_signal_to_continue() -> None:
    async def on_partitions_revoked_callback_side_effect(
        revoked: List[TopicPartition], rebalancer: DefaultRebalancer
    ) -> None:
        rebalancer.continue_with_partition_assignment()

    on_partitions_revoked_callback = AsyncMock(
        side_effect=on_partitions_revoked_callback_side_effect
    )
    on_partitions_assigned_callback = AsyncMock()

    rebalancer = DefaultRebalancer(
        on_partitions_revoked_callback=on_partitions_revoked_callback,
        on_partitions_assigned_callback=on_partitions_assigned_callback,
    )

    await rebalancer.on_partitions_revoked(
        [TopicPartition("email_sent_v1", 0)]
    )

    on_partitions_revoked_callback.assert_awaited_once_with(
        [TopicPartition("email_sent_v1", 0)], rebalancer
    )


@pytest.mark.asyncio
async def test_default_rebalancer_calls_user_on_partitions_assigned_callback() -> None:
    on_partitions_revoked_callback = AsyncMock()
    on_partitions_assigned_callback = AsyncMock()

    rebalancer = DefaultRebalancer(
        on_partitions_revoked_callback=on_partitions_revoked_callback,
        on_partitions_assigned_callback=on_partitions_assigned_callback,
    )

    await rebalancer.on_partitions_assigned(
        [TopicPartition("email_sent_v1", 0)]
    )

    on_partitions_assigned_callback.assert_awaited_once_with(
        [TopicPartition("email_sent_v1", 0)], rebalancer
    )
