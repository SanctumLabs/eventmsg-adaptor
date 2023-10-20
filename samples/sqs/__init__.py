from samples import config

if config.adapters and config.adapters.sqs:
    if not config.adapters.sqs.environment:
        raise Exception(
            "You must must first configure your environment in your SQSConfig. eg: EVENTING_ADAPTERS__SQS__ENVIRONMENT=development"
        )
