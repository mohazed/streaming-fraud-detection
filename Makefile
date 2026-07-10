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

run-local:
	@echo "run-local: not implemented"; exit 1

train:
	@echo "train: not implemented"; exit 1

api:
	@echo "api: not implemented"; exit 1

smoke:
	@echo "smoke: not implemented"; exit 1
