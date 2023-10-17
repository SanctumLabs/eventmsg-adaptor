import pytest

from tests import make_mock_event
from eventmsg_adaptor.schema import PydanticEventBody


def test_event_payload_returns_body_for_backwards_compatibility() -> None:
    event = make_mock_event(destination="shop")
    assert event.payload == event.body


def test_pydantic_event_body_allows_for_arbitrary_fields() -> None:
    event_body = PydanticEventBody(foo="bar")  # type: ignore[call-arg]

    assert event_body.foo == "bar"  # type: ignore[attr-defined]


def test_pydantic_event_body_event_name_raises_not_implemented_error() -> None:
    event_body = PydanticEventBody()

    with pytest.raises(NotImplementedError):
        event_body.event_name
