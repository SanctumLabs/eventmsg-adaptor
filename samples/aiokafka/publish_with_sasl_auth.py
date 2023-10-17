#!/usr/bin/env python
import asyncio
import signal
from datetime import datetime
from typing import cast

from google.protobuf.timestamp_pb2 import Timestamp
from sanctumlabs.messageschema.events.notifications.email.v1.events_pb2 import (
    EmailReceived,
)
from sanctumlabs.messageschema.events.notifications.email.v1.data_pb2 import (
    Email,
    EmailStatus,
)
from sanctumlabs.messageschema.messages.notifications.email.v1.events_pb2 import EmailV1

from eventmsg_adaptor import factory
from eventmsg_adaptor.config import AdapterConfigs, Config
from eventmsg_adaptor.config.kafka import KafkaConfig, KafkaSecurityProtocolConfig
from eventmsg_adaptor.event_streams import AsyncEventStream

event_loop = asyncio.get_event_loop()

config = Config(
    adapters=AdapterConfigs(
        kafka=KafkaConfig(
            bootstrap_servers=["localhost:9092"],
            security=KafkaSecurityProtocolConfig(
                sasl_username="xxx",
                sasl_password="xxx",
            ),
        )
    ),
)

event_stream = cast(AsyncEventStream, factory(config, adapter_name="aiokafka"))


async def shutdown_handler() -> None:
    await event_stream.close()

    event_loop.stop()


for sig in [signal.SIGHUP, signal.SIGTERM, signal.SIGINT]:
    event_loop.add_signal_handler(
        sig,
        lambda sig=sig: event_loop.create_task(shutdown_handler()),
    )


async def publish_event() -> None:
    started_at_timestamp = Timestamp()
    started_at_timestamp.FromDatetime(datetime.now())

    await event_stream.publish(
        "email_v1",
        EmailV1(
            email_received=EmailReceived(
                email=Email(
                    id="fae9ef15-06f0-46b9-95f1-2a75dd482687",
                    to="bot@example.com",
                    subject="Testing 123",
                    message="Robot Schematics",
                    status=EmailStatus.PENDING,
                )
            )
        ),
    ),


try:
    event_loop.run_until_complete(publish_event())
except KeyboardInterrupt:
    pass
finally:
    event_loop.close()
