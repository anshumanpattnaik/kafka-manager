lint:
	pylint src tests

test:
	pytest tests --junitxml=all_junit.xml

test-unit:
	pytest tests/unit --junitxml=unit_junit.xml

test-coverage:
	pytest --cov=src --cov-report html --cov-report term --cov-report xml tests/unit
