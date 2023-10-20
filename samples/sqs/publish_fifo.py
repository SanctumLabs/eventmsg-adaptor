#!/usr/bin/env python
from typing import cast
from uuid import uuid4

import eventmsg_adaptor
from samples import config
from samples.events import business_registered_event
from eventmsg_adaptor.event_streams import EventStream

event_stream = cast(EventStream, eventmsg_adaptor.factory(config, adapter_name="sqs"))

# We use the same sample `business_registered_event`; however we must generate a new random business id because
# we use content-based deduplication in SQS
business_registered_event = business_registered_event.copy(
    update={"business_id": uuid4()}
)

event_stream.publish(
    "merchant_profile.payment::fifo",
    business_registered_event,
    group_id=str(business_registered_event.business_id),
)
