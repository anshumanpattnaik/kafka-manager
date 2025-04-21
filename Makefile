lint:
	pylint kafka_manager tests

test:
	pytest tests --junitxml=all_junit.xml

test-unit:
	pytest tests/unit --junitxml=unit_junit.xml

test-coverage:
	pytest --cov=kafka_manager --cov-report html --cov-report term --cov-report xml tests/unit
