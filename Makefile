PYTHON  ?= python3
COMPOSE ?= docker compose

# Derived from the installed pyspark so the connector package can never drift
# out of sync with the pinned pyspark version (used from Phase 2 onward).
SPARK_VERSION      := $(shell $(PYTHON) -c "import pyspark; print(pyspark.__version__)" 2>/dev/null)
SPARK_KAFKA_PACKAGE := org.apache.spark:spark-sql-kafka-0-10_2.12:$(SPARK_VERSION)

.PHONY: up down topics test test-unit cov clean run-local train api smoke

up:
	$(COMPOSE) up -d
	@echo "waiting for kafka..."
	@until $(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1; do sleep 2; done
	@echo "kafka is up"

down:
	$(COMPOSE) down -v

topics:
	$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
		--topic transactions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
	$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
		--topic fraud-alerts --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
	$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

test:
	$(PYTHON) -m pytest

test-unit:
	$(PYTHON) -m pytest -m "not spark"

cov:
	$(PYTHON) -m pytest --cov=common --cov=producer --cov=spark --cov=api \
		--cov-report=term-missing --cov-fail-under=85

clean:
	find data/checkpoints data/out -mindepth 1 ! -name .gitkeep -exec rm -rf {} +
	rm -rf .pytest_cache .coverage htmlcov
	find . -type d -name __pycache__ -exec rm -rf {} +

# Runs producer + spark/job.py + api against a Kafka you already brought up
# with `make up && make topics` (KAFKA_BOOTSTRAP_SERVERS defaults to
# localhost:9092). Needs JAVA_HOME pointed at a JDK 17 for the pandas_udf.
# Ctrl-C the api target to stop.
run-local:
	$(PYTHON) -m producer.producer --rate 50 --seed 42 --limit 200000 &
	PYTHONPATH=$(CURDIR) spark-submit --packages $(SPARK_KAFKA_PACKAGE) spark/job.py &
	$(PYTHON) -m uvicorn api.main:app --host 0.0.0.0 --port 8000

train:
	$(PYTHON) -m ml.train

api:
	$(PYTHON) -m uvicorn api.main:app --host 0.0.0.0 --port 8000

# Brings its own Kafka up via docker compose, drives a real
# producer + spark-submit + api against it, and tears everything down again -
# see scripts/smoke_test.py.
smoke:
	$(PYTHON) scripts/smoke_test.py
