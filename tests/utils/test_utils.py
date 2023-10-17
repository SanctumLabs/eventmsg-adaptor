from typing import Union

import pytest

from eventmsg_adaptor.schema import Destination, ListenExpression
from eventmsg_adaptor.utils import (
    normalise_destination,
    parse_destination_str,
    parse_listen_expression_str,
)


@pytest.mark.parametrize(
    "destination, expected_destination",
    [
        (Destination(topic="shop"), Destination(topic="shop")),
        ("shop", Destination(topic="shop")),
        ("shop.", Destination(topic="shop")),
        ("shop.*", Destination(topic="shop")),
        ("shop.order", Destination(topic="shop", sub_topic="order")),
        ("shop.order.*", Destination(topic="shop", sub_topic="order")),
        ("shop::fifo", Destination(topic="shop", is_fifo=True)),
        ("shop.*::fifo", Destination(topic="shop", is_fifo=True)),
        (
            "shop.order::fifo",
            Destination(topic="shop", sub_topic="order", is_fifo=True),
        ),
        (
            "shop.order.*::fifo",
            Destination(topic="shop", sub_topic="order", is_fifo=True),
        ),
    ],
)
def test_normalise_destination(
    destination: Union[str, Destination], expected_destination: Destination
) -> None:
    assert normalise_destination(destination) == expected_destination
