.PHONY: test
test:
	python3 -m unittest discover -s rudder_airflow_provider/test 
