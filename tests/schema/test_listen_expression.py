import pytest

from tests import make_mock_event
from eventmsg_adaptor.schema import (
    Destination,
    ListenExpression
)

@pytest.mark.parametrize(
    "listen_expression, expected_str",
    [
        (ListenExpression(destination=Destination(topic="shop")), "shop"),
        (
            ListenExpression(destination=Destination(topic="shop", sub_topic="order")),
            "shop.order",
        ),
        (
            ListenExpression(
                destination=Destination(topic="shop"), event_name="order_placed"
            ),
            "shop/order_placed",
        ),
        (
            ListenExpression(
                destination=Destination(topic="shop"),
                event_name="order_placed",
                version="1.0",
            ),
            "shop/order_placed/1.0",
        ),
        (
            ListenExpression(
                destination=Destination(topic="shop", sub_topic="order"),
                event_name="order_placed",
            ),
            "shop.order/order_placed",
        ),
        (
            ListenExpression(
                destination=Destination(topic="shop", sub_topic="order"),
                event_name="order_placed",
                version="1.0",
            ),
            "shop.order/order_placed/1.0",
        ),
        (
            ListenExpression(destination=Destination(topic="shop", is_fifo=True)),
            "shop::fifo",
        ),
        (
            ListenExpression(
                destination=Destination(topic="shop", sub_topic="order", is_fifo=True)
            ),
            "shop.order::fifo",
        ),
        (
            ListenExpression(
                destination=Destination(topic="shop", is_fifo=True),
                event_name="order_placed",
            ),
            "shop::fifo/order_placed",
        ),
        (
            ListenExpression(
                destination=Destination(topic="shop", is_fifo=True),
                event_name="order_placed",
                version="1.0",
            ),
            "shop::fifo/order_placed/1.0",
        ),
        (
            ListenExpression(
                destination=Destination(topic="shop", sub_topic="order", is_fifo=True),
                event_name="order_placed",
                version="1.0",
            ),
            "shop.order::fifo/order_placed/1.0",
        ),
    ],
)
def test_listen_expression_str(
    listen_expression: ListenExpression, expected_str: str
) -> None:
    assert str(listen_expression) == expected_str


def test_listen_expression_matches() -> None:
    listen_expression = ListenExpression(destination=Destination(topic="shop"))
    assert listen_expression.matches(make_mock_event(destination="shop"))
    assert listen_expression.matches(make_mock_event(destination="shop.order"))
    assert listen_expression.matches(make_mock_event(destination="shop.cart"))
    assert not (
        listen_expression.matches(
            make_mock_event(destination="merchant_account.transactions")
        )
    )

    listen_expression = ListenExpression(
        destination=Destination(topic="shop", sub_topic="order")
    )
    assert listen_expression.matches(make_mock_event(destination="shop.order"))
    assert not listen_expression.matches(make_mock_event(destination="shop.cart"))
    assert not listen_expression.matches(
        make_mock_event(destination="merchant_account.transactions")
    )

    listen_expression = ListenExpression(
        destination=Destination(topic="shop"), event_name="order_placed"
    )
    assert listen_expression.matches(
        make_mock_event(destination="shop", event_name="order_placed")
    )
    assert listen_expression.matches(
        make_mock_event(destination="shop.order", event_name="order_placed")
    )
    assert listen_expression.matches(
        make_mock_event(destination="shop.cart", event_name="order_placed")
    )
    assert not (
        listen_expression.matches(
            make_mock_event(
                destination="merchant_account.transactions", event_name="order_placed"
            )
        )
    )
    assert not (
        listen_expression.matches(
            make_mock_event(destination="shop", event_name="order_paid")
        )
    )

    listen_expression = ListenExpression(
        destination=Destination(topic="shop", sub_topic="order"),
        event_name="order_placed",
    )
    assert listen_expression.matches(
        make_mock_event(destination="shop.order", event_name="order_placed")
    )
    assert not (
        listen_expression.matches(
            make_mock_event(destination="shop.cart", event_name="order_placed")
        )
    )
    assert not (
        listen_expression.matches(
            make_mock_event(
                destination="merchant_account.transactions", event_name="order_placed"
            )
        )
    )
    assert not (
        listen_expression.matches(
            make_mock_event(destination="shop", event_name="order_paid")
        )
    )
    assert not (
        listen_expression.matches(
            make_mock_event(destination="shop.order", event_name="order_paid")
        )
    )

    listen_expression = ListenExpression(
        destination=Destination(topic="shop", is_fifo=True)
    )
    assert listen_expression.matches(make_mock_event(destination="shop::fifo"))
    assert listen_expression.matches(make_mock_event(destination="shop.order::fifo"))
    assert listen_expression.matches(make_mock_event(destination="shop.cart::fifo"))
    assert not (
        listen_expression.matches(
            make_mock_event(destination="merchant_account.transactions::fifo")
        )
    )
    assert not listen_expression.matches(make_mock_event(destination="shop"))
    assert not (listen_expression.matches(make_mock_event(destination="shop.order")))
    assert not listen_expression.matches(make_mock_event(destination="shop.cart"))
    assert not (
        listen_expression.matches(
            make_mock_event(destination="merchant_account.transactions")
        )
    )

    listen_expression = ListenExpression(
        destination=Destination(topic="shop", sub_topic="order", is_fifo=True)
    )
    assert listen_expression.matches(make_mock_event(destination="shop.order::fifo"))
    assert not (
        listen_expression.matches(make_mock_event(destination="shop.cart::fifo"))
    )
    assert not (
        listen_expression.matches(
            make_mock_event(destination="merchant_account.transactions::fifo")
        )
    )
    assert not (listen_expression.matches(make_mock_event(destination="shop.order")))
    assert not listen_expression.matches(make_mock_event(destination="shop.cart"))
    assert not (
        listen_expression.matches(
            make_mock_event(destination="merchant_account.transactions")
        )
    )

    listen_expression = ListenExpression(
        destination=Destination(topic="shop", is_fifo=True), event_name="order_placed"
    )
    assert listen_expression.matches(
        make_mock_event(destination="shop::fifo", event_name="order_placed")
    )
    assert listen_expression.matches(
        make_mock_event(destination="shop.order::fifo", event_name="order_placed")
    )
    assert listen_expression.matches(
        make_mock_event(destination="shop.cart::fifo", event_name="order_placed")
    )
    assert not (
        listen_expression.matches(
            make_mock_event(
                destination="merchant_account.transactions::fifo",
                event_name="order_placed",
            )
        )
    )
    assert not (
        listen_expression.matches(
            make_mock_event(destination="shop::fifo", event_name="order_paid")
        )
    )
    assert not (
        listen_expression.matches(
            make_mock_event(destination="shop", event_name="order_placed")
        )
    )
    assert not (
        listen_expression.matches(
            make_mock_event(destination="shop.order", event_name="order_placed")
        )
    )
    assert not (
        listen_expression.matches(
            make_mock_event(destination="shop.cart", event_name="order_placed")
        )
    )
    assert not (
        listen_expression.matches(
            make_mock_event(
                destination="merchant_account.transactions", event_name="order_placed"
            )
        )
    )
    assert not (
        listen_expression.matches(
            make_mock_event(destination="shop", event_name="order_paid")
        )
    )
