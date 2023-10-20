# SQS Samples

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

## Running

``` shell
poetry run python -m samples.sqs.publish
poetry run python -m samples.sqs.subscribe

poetry run python -m samples.sqs.publish_fifo
poetry run python -m samples.sqs.subscribe_fifo
```
