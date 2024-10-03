.PHONY: test
test:
	pytest --cov=rudder_airflow_provider rudder_airflow_provider/test --cov-report=xml
