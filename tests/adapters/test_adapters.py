from typing import Any, cast
from unittest import mock
from unittest.mock import Mock, call, create_autospec

import pytest

# TODO: uncomment once the aiobotocore is installed correctly
# from aiobotocore.session import AioSession
# from mypy_boto3_sns import SNSServiceResource
# from mypy_boto3_sqs import SQSServiceResource

from eventmsg_adaptor.adapters import AIOKafkaAdapter, factory
from eventmsg_adaptor.config import AdapterConfigs, Config
from eventmsg_adaptor.config.kafka import (
    KafkaConfig,
    KafkaSecurityProtocolConfig,
)


def test_factory_raises_an_exception_when_no_adapters_are_configured() -> None:
    with pytest.raises(
        Exception, match="You must configure at least one adapter in the config."
    ):
        factory("sqs", Config(adapters=None))


def test_factory_raises_an_exception_when_creating_an_sqs_adapter_and_sqs_config_is_not_defined() -> (
    None
):
    with pytest.raises(
        Exception, match="You must specify an SQSConfig when using the sqs adapter."
    ):
        factory("sqs", Config(adapters=AdapterConfigs(sqs=None)))


def test_factory_raises_an_exception_when_creating_an_aiosqs_adapter_and_sqs_config_is_not_defined() -> (
    None
):
    with pytest.raises(
        Exception, match="You must specify an SQSConfig when using the aiosqs adapter."
    ):
        factory("aiosqs", Config(adapters=AdapterConfigs(sqs=None)))


def test_factory_raises_an_exception_when_creating_an_aiokafka_adapter_and_kafka_config_is_not_defined() -> (
    None
):
    with pytest.raises(
        Exception,
        match="You must specify a KafkaConfig when using the aiokafka adapter.",
    ):
        factory("aiokafka", Config(adapters=AdapterConfigs(kafka=None)))


def test_factory_raises_an_exception_when_adapter_type_is_not_supported() -> None:
    with pytest.raises(
        Exception,
        match="The adapter foobar is not supported.",
    ):
        factory("foobar", Config())


# TODO: uncomment once there is an SQSAdapter
# @mock.patch("boto3.resource")
# def test_factory_creates_an_sqs_adapter(mock_create_boto3_resource: Mock) -> None:
#     sqs_resource = create_autospec(SQSServiceResource)
#     sns_resource = create_autospec(SNSServiceResource)

#     def create_resource_side_effect(
#         service_name: str, *args: Any, **kwargs: Any
#     ) -> Any:
#         if service_name == "sqs":
#             return sqs_resource
#         elif service_name == "sns":
#             return sns_resource
#         else:
#             raise Exception(f"The resource {service_name} is unknown.")

#     mock_create_boto3_resource.side_effect = create_resource_side_effect

#     adapter = cast(
#         SQSAdapter,
#         factory(
#             "sqs",
#             Config(
#                 service_name="hello",
#                 adapters=AdapterConfigs(
#                     sqs=SQSConfig(
#                         region_name="eu-west-2",
#                         access_key_id="MyAccessKey",
#                         secret_key="ThisIsVerySecret",
#                         environment="dev",
#                     )
#                 ),
#             ),
#         ),
#     )

#     assert adapter.sqs is sqs_resource
#     assert adapter.sns is sns_resource
#     assert adapter.topic_prefix == "dev"
#     assert adapter.subscription_prefix == "dev-hello"
#     assert adapter.subscriber_wait_time_in_seconds == 2
#     assert adapter.max_number_of_messages_to_return == 5

#     mock_create_boto3_resource.assert_has_calls(
#         [
#             call(
#                 "sqs",
#                 region_name="eu-west-2",
#                 aws_secret_access_key="ThisIsVerySecret",
#                 aws_access_key_id="MyAccessKey",
#             ),
#             call(
#                 "sns",
#                 region_name="eu-west-2",
#                 aws_secret_access_key="ThisIsVerySecret",
#                 aws_access_key_id="MyAccessKey",
#             ),
#         ],
#     )
#
#
# @mock.patch("boto3.resource")
# def test_factory_creates_an_sqs_adapter_when_no_environment_is_set(
#     mock_create_boto3_resource: Mock,
# ) -> None:
#     sqs_resource = create_autospec(SQSServiceResource)
#     sns_resource = create_autospec(SNSServiceResource)

#     def create_resource_side_effect(
#         service_name: str, *args: Any, **kwargs: Any
#     ) -> Any:
#         if service_name == "sqs":
#             return sqs_resource
#         elif service_name == "sns":
#             return sns_resource
#         else:
#             raise Exception(f"The resource {service_name} is unknown.")

#     mock_create_boto3_resource.side_effect = create_resource_side_effect

#     adapter = cast(
#         SQSAdapter,
#         factory(
#             "sqs",
#             Config(
#                 service_name="hello",
#                 adapters=AdapterConfigs(
#                     sqs=SQSConfig(
#                         region_name="eu-west-2",
#                         access_key_id="MyAccessKey",
#                         secret_key="ThisIsVerySecret",
#                     )
#                 ),
#             ),
#         ),
#     )

#     assert adapter.sqs is sqs_resource
#     assert adapter.sns is sns_resource
#     assert adapter.topic_prefix is None
#     assert adapter.subscription_prefix == "hello"
#     assert adapter.subscriber_wait_time_in_seconds == 2
#     assert adapter.max_number_of_messages_to_return == 5

#     mock_create_boto3_resource.assert_has_calls(
#         [
#             call(
#                 "sqs",
#                 region_name="eu-west-2",
#                 aws_secret_access_key="ThisIsVerySecret",
#                 aws_access_key_id="MyAccessKey",
#             ),
#             call(
#                 "sns",
#                 region_name="eu-west-2",
#                 aws_secret_access_key="ThisIsVerySecret",
#                 aws_access_key_id="MyAccessKey",
#             ),
#         ],
#     )

# TODO: uncomment once aiobotocore is setup & there is an AIOSQSAdaptor
# @mock.patch("aiobotocore.session.get_session")
# def test_factory_creates_an_aiosqs_adapter(mock_get_aiobotocore_session: Mock) -> None:
#     session = create_autospec(AioSession)

#     mock_get_aiobotocore_session.return_value = session

#     adapter = cast(
#         AIOSQSAdapter,
#         factory(
#             "aiosqs",
#             Config(
#                 service_name="hello-svc",
#                 adapters=AdapterConfigs(
#                     sqs=SQSConfig(
#                         region_name="eu-west-2",
#                         access_key_id="MyAccessKey",
#                         secret_key="ThisIsVerySecret",
#                         environment="dev",
#                     )
#                 ),
#             ),
#         ),
#     )

#     assert adapter.session is session
#     assert adapter.aws_region_name == "eu-west-2"
#     assert adapter.aws_access_key_id == "MyAccessKey"
#     assert adapter.aws_secret_key == "ThisIsVerySecret"
#     assert adapter.topic_prefix == "dev"
#     assert adapter.subscription_prefix == "dev-hello"
#     assert adapter.subscriber_wait_time_in_seconds == 2
#     assert adapter.max_number_of_messages_to_return == 5

#     mock_get_aiobotocore_session.assert_called_once_with()


# @mock.patch("aiobotocore.session.get_session")
# def test_factory_creates_an_aiosqs_adapter_when_no_environment_is_set(
#     mock_get_aiobotocore_session: Mock,
# ) -> None:
#     session = create_autospec(AioSession)

#     mock_get_aiobotocore_session.return_value = session

#     adapter = cast(
#         AIOSQSAdapter,
#         factory(
#             "aiosqs",
#             Config(
#                 service_name="hello",
#                 adapters=AdapterConfigs(
#                     sqs=SQSConfig(
#                         region_name="eu-west-2",
#                         access_key_id="MyAccessKey",
#                         secret_key="ThisIsVerySecret",
#                     )
#                 ),
#             ),
#         ),
#     )

#     assert adapter.session is session
#     assert adapter.aws_region_name == "eu-west-2"
#     assert adapter.aws_access_key_id == "MyAccessKey"
#     assert adapter.aws_secret_key == "ThisIsVerySecret"
#     assert adapter.topic_prefix is None
#     assert adapter.subscription_prefix == "hello"
#     assert adapter.subscriber_wait_time_in_seconds == 2
#     assert adapter.max_number_of_messages_to_return == 5

#     mock_get_aiobotocore_session.assert_called_once_with()


@pytest.mark.asyncio
async def test_factory_creates_an_aiokafka_adapter() -> None:
    adapter = cast(
        AIOKafkaAdapter,
        factory(
            "aiokafka",
            Config(
                service_name="hello",
                adapters=AdapterConfigs(
                    kafka=KafkaConfig(
                        bootstrap_servers=["localhost:9092"]
                    )
                ),
            ),
        ),
    )

    assert adapter.bootstrap_servers == ["localhost:9092"]
    assert adapter.group_id == "hello"


@pytest.mark.asyncio
async def test_factory_creates_an_aiokafka_adapter_with_sasl_auth() -> None:
    adapter = cast(
        AIOKafkaAdapter,
        factory(
            "aiokafka",
            Config(
                service_name="hello",
                adapters=AdapterConfigs(
                    kafka=KafkaConfig(
                        bootstrap_servers=["localhost:9092"],
                        security=KafkaSecurityProtocolConfig(
                            sasl_username="foo",
                            sasl_password="bar",
                        ),
                    )
                ),
            ),
        ),
    )

    assert adapter.bootstrap_servers == ["localhost:9092"]
    assert adapter.group_id == "hello"
    assert adapter.sasl_username == "foo"
    assert adapter.sasl_password == "bar"
