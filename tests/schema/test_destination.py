import pytest

from eventmsg_adaptor.schema import Destination


@pytest.mark.parametrize(
    "destination, expected_path",
    [
        (Destination(topic="shop"), "shop"),
        (Destination(topic="shop", sub_topic="order"), "shop.order"),
        (Destination(topic="shop", sub_topic="order", is_fifo=True), "shop.order"),
    ],
)
def test_destination_path(destination: Destination, expected_path: str) -> None:
    assert destination.path == expected_path


@pytest.mark.parametrize(
    "destination, expected_str",
    [
        (Destination(topic="shop"), "shop"),
        (Destination(topic="shop", sub_topic="order"), "shop.order"),
        (Destination(topic="shop", sub_topic="order", is_fifo=True), "shop.order::fifo"),
        (Destination(topic="shop", is_fifo=True), "shop::fifo"),
    ],
)
def test_destination_str(destination: Destination, expected_str: str) -> None:
    assert str(destination) == expected_str
