# Samples

## Installing

To run the samples:

1. Clone the repository
2. Run `poetry install` in the root of the repo.
3. Create a `.env` file in the `samples/` directory with the following:

    ``` plain
    EVENTING_ADAPTERS__SQS__ACCESS_KEY_ID=xxxxxx
    EVENTING_ADAPTERS__SQS__ENVIRONMENT=yourfullnamehere (eg: businessman)
    EVENTING_ADAPTERS__SQS__SECRET_KEY=xxxxxx
    ```

4. For testing the `AIOKafkaAdapter` locally, you can run the full confluent-kafka suite in docker using the bundled
   [docker-compose.yml](./aiokafka/docker-compose.yml)

## Running

``` shell
poetry run python -m samples.sqs.publish
poetry run python -m samples.sqs.subscribe

poetry run python -m samples.sqs.publish_fifo
poetry run python -m samples.sqs.subscribe_fifo

poetry run python -m samples.aiosqs.publish
poetry run python -m samples.aiosqs.subscribe

poetry run python -m samples.aiosqs.publish_fifo
poetry run python -m samples.aiosqs.subscribe_fifo

poetry run python -m samples.aiokafka.publish_with_sasl_auth
poetry run python -m samples.aiokafka.subscribe_with_sasl_auth

poetry run python -m samples.aiokafka.publish_using_schema_registry

poetry run python -m samples.aiokafka.publish
poetry run python -m samples.aiokafka.subscribe
```
