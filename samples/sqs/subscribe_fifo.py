#!/usr/bin/env python
from typing import cast

import eventmsg_adaptor
from samples import config, print_event
from eventmsg_adaptor.event_streams import EventStream
from eventmsg_adaptor.schema import Event

event_stream = cast(EventStream, eventmsg_adaptor.factory(config, adapter_name="sqs"))


@event_stream.subscribe("merchant_profile.payment::fifo")
def listener(event: Event) -> None:
    print_event(event)
