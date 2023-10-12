# AIOKafka Samples

## Installing

To run this sample:

1. Clone the repository
2. Run `poetry install` in the root of the repo.
3. Change directory to the `samples/aiokafka` directory:

   ```shell
   cd samples/aiokakfa
   ```

4. Run `docker compose up` to run Kafka suite.

## Running

From the current directory(i.e. samples/aiokafka), you can run the following:

``` shell
poetry run python -m publish_with_sasl_auth
poetry run python -m subscribe_with_sasl_auth

poetry run python -m publish_using_schema_registry

poetry run python -m publish
poetry run python -m subscribe
```
