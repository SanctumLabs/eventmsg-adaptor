from eventmsg_adaptor.config.sqs import SQSConfig


def test_sqs_config_allows_optional_credentials_to_fallback_on_assumed_iam_role() -> (
    None
):
    SQSConfig(access_key_id=None, secret_key=None)
