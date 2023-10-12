#!/usr/bin/env python
import asyncio
import signal
from typing import cast

from sanctumlabs.messageschema.messages.notifications.email.v1.events_pb2 import EmailV1

from eventmsg_adaptor import factory
from samples import print_event
from eventmsg_adaptor.config import AdapterConfigs, Config, KafkaConfig
from eventmsg_adaptor.event_streams import AsyncEventStream
from eventmsg_adaptor.schema import Event

event_loop = asyncio.get_event_loop()

config = Config(
    service_name="consumer_",
    adapters=AdapterConfigs(
        kafka=KafkaConfig(
            bootstrap_server1_host="localhost",
            bootstrap_server1_port=9092,
            sasl_username="xxx",
            sasl_password="xxx",
        )
    ),
)


async def subscriber_exception_handler(e: Exception) -> None:
    print(e)


event_stream = cast(
    AsyncEventStream,
    factory(
        config,
        adapter_name="aiokafka",
        subscriber_exception_handler=subscriber_exception_handler,
    ),
)


async def shutdown_handler() -> None:
    await event_stream.close()

    event_loop.stop()


for sig in [signal.SIGHUP, signal.SIGTERM, signal.SIGINT]:
    event_loop.add_signal_handler(
        sig,
        lambda sig=sig: event_loop.create_task(shutdown_handler()),
    )


@event_stream.subscribe("email_v1")
async def listener(event: Event[EmailV1]) -> None:
    print_event(event)


try:
    asyncio.ensure_future(event_stream.listen())
    event_loop.run_forever()
except KeyboardInterrupt:
    pass
finally:
    event_loop.close()
