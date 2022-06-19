.PHONY: build-tests run-tests clean-tests


test: clean-tests build-tests run-tests clean-tests

build-tests:
	docker-compose -f docker-compose.tests.yaml build

run-tests:
	docker-compose -f docker-compose.tests.yaml run pqueue

clean-tests:
	docker-compose -f docker-compose.tests.yaml down --remove-orphans
