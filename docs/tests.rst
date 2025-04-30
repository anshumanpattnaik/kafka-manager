Tests
############
The Testsuite includes Unit Tests and Integration Tests, with various test cases that cover many test scenarios and check the efficiency of the Kafka Manager tool.

Unit tests
**********
The unit test suite mocks the producer client, consumer client, admin client, Kafka producer, and consumer for testing.

The test suite is run via pytest, and to run the test locally, install the test dependencies.

.. code:: bash

    pip install -r requirements-dev.txt

Then run the test with the following pytest command from your preferred Python virtual environment.

.. code:: bash

    pytest -v ./tests/unit

Integration Tests
*****************
The integration tests that setup and teardown Kafka manager, Kafka Admin, Producer Client & Consumer Client for testing. To run the tests, start the Kafka broker docker container.

Run the following command to start the docker container, and docker-compose resides inside the tests folder.

.. code:: bash

    docker-compose up -d

The test suite is run via pytest. To run the test locally, use the following pytest command from your preferred Python virtual environment.

.. code:: bash

    pytest -v ./tests/integration
