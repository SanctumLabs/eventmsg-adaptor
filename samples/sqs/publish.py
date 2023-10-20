#!/usr/bin/env python
from typing import cast

import eventmsg_adaptor
from samples import config
from samples.events import business_registered_event
from eventmsg_adaptor.event_streams import EventStream

event_stream = cast(EventStream, eventmsg_adaptor.factory(config, adapter_name="sqs"))

event_stream.publish("merchant_profile.business", business_registered_event)
