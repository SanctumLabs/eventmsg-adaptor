from typing import Generator, Any
from datetime import datetime
from unittest import mock
from unittest.mock import MagicMock, create_autospec
import uuid
import pytest
import time_machine

from confluent_kafka.schema_registry import SchemaRegistryClient, RegisteredSchema

@pytest.fixture(autouse=True)
def auto_freeze_time() -> Generator[Any, Any, None]:
    with time_machine.travel(
        destination=datetime(2020, 9, 8, 19, 53, 46).astimezone(),
        tick=False
    ):
        yield

@pytest.fixture(autouse=True)
def fix_uuid4() -> Generator[Any, Any, None]:
    with mock.patch.object(
        target=uuid,
        attribute="uuid4",
        return_value=uuid.UUID("fae9ef15-06f0-46b9-95f1-2a75dd482687"),
    ):
        yield
        
@pytest.fixture()
def mock_schema_registry_client() -> Generator[MagicMock, Any, None]:
    mock_schema_registry_client = create_autospec(SchemaRegistryClient)

    mock_schema = RegisteredSchema(
        schema_id=100028,
        schema=MagicMock(),
        subject="sanctumlabs/events/signup/v1/events.proto",
        version=1,
    )

    mock_schema_registry_client.lookup_schema = MagicMock(
        return_value=mock_schema,
    )

    mock_schema_registry_client.get_latest_version = MagicMock(return_value=mock_schema)

    yield mock_schema_registry_client
