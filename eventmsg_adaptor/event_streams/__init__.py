from typing import Any, Optional, Union

from eventmsg_adaptor.adapters import factory as adapter_factory
from eventmsg_adaptor.adapters.base import BaseAsyncAdapter
from eventmsg_adaptor.config import Config
from eventmsg_adaptor.event_streams.async_event_stream import AsyncEventStream
from eventmsg_adaptor.event_streams.event_stream import EventStream


def factory(
    config: Config, adapter_name: Optional[str] = None, **kwargs: Any
) -> Union[EventStream, AsyncEventStream]:
    adapter_name = adapter_name or config.default_adapter

    adapter = adapter_factory(adapter_name, config=config)

    if isinstance(adapter, BaseAsyncAdapter):
        event_loop = kwargs.get("event_loop")
        subscriber_exception_handler = kwargs.get("subscriber_exception_handler")

        # @deprecated: `listen_exception_handler` is deprecated. Please use `subscriber_exception_handler` as a drop-in replacement.
        if not subscriber_exception_handler:
            subscriber_exception_handler = kwargs.get("listen_exception_handler")

        return AsyncEventStream(
            adapter=adapter,
            event_source=config.service_name,
            subscriber_exception_handler=subscriber_exception_handler,
            event_loop=event_loop,
        )
    else:
        return EventStream(adapter=adapter, event_source=config.service_name)


__all__ = ["AsyncEventStream", "EventStream", "factory"]
