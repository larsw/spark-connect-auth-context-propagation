# Spark Connect token + correlation-ID propagation PoC
#
# Typical first run:
#   ./install.sh && make build && make up && make demo

SHELL := /bin/bash
COMPOSE := docker compose
JAR_SRC := server/target/spark-connect-propagation-0.1.0.jar
JAR_DST := docker/spark/jars/spark-connect-propagation-0.1.0.jar

.DEFAULT_GOAL := help
.PHONY: help install jar build up bootstrap demo test-unit test test-container logs cid ps down clean

help: ## Show this help
	@echo "Spark Connect propagation PoC"
	@echo
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[1m%-12s\033[0m %s\n", $$1, $$2}'
	@echo

install: ## Check the toolchain and /etc/hosts aliases (prompts before anything privileged)
	@./install.sh

jar: ## Build the Java plugin jar and stage it for the image
	@mvn -q -f server/pom.xml package
	@mkdir -p docker/spark/jars
	@cp $(JAR_SRC) $(JAR_DST)
	@echo "staged $(JAR_DST)"

build: jar ## Build the plugin jar and the Spark image
	@$(COMPOSE) build

up: ## Start the whole stack and wait until Spark Connect is accepting connections
	@$(COMPOSE) up -d
	@echo "waiting for spark-connect to become healthy ..."
	@for i in $$(seq 1 90); do \
	  status=$$($(COMPOSE) ps --format json spark-connect 2>/dev/null | python3 -c 'import sys,json; print(json.loads(sys.stdin.read() or "{}").get("Health",""))' 2>/dev/null); \
	  if [ "$$status" = "healthy" ]; then echo "stack is up"; exit 0; fi; \
	  sleep 2; \
	done; \
	echo "spark-connect did not become healthy; try 'make logs'" >&2; exit 1
	@echo
	@echo "  Keycloak       http://keycloak:8080       (admin/admin)"
	@echo "  Polaris        http://polaris:8181"
	@echo "  MinIO console  http://minio:9001          (minio_root/m1n1opwd)"
	@echo "  Spark master   http://spark-master:8082"
	@echo "  Spark driver   http://spark-connect:4040"
	@echo "  Spark Connect  sc://spark-connect:15002"

bootstrap: ## Re-run the Polaris catalog/principal/grant bootstrap (idempotent)
	@$(COMPOSE) run --rm polaris-setup

demo: ## Interactive two-user walkthrough (device flow; opens a browser URL)
	@cd client && uv run python ../demo/demo.py

test-unit: ## Fast client unit tests -- no stack, no docker, no network
	@cd client && uv run pytest ../tests/test_client_unit.py -q

test: ## Full suite on the host (needs ./install.sh for the /etc/hosts aliases)
	@cd client && uv run pytest ../tests -v

test-container: ## Full suite inside the compose network (no host setup needed)
	@$(COMPOSE) --profile tools build client
	@$(COMPOSE) run --rm --entrypoint python client -m pytest /work/tests -v

logs: ## Follow logs from every service
	@$(COMPOSE) logs -f

cid: ## Trace one correlation ID across all three services: make cid CID=<uuid>
	@test -n "$(CID)" || { echo "usage: make cid CID=<uuid>" >&2; exit 2; }
	@for svc in spark-connect polaris; do \
	  echo "----- $$svc -----"; \
	  $(COMPOSE) logs --no-log-prefix $$svc 2>/dev/null | grep -F "$(CID)" || echo "  (no match)"; \
	done

ps: ## Show service status
	@$(COMPOSE) ps

down: ## Stop the stack and remove volumes
	@$(COMPOSE) down -v --remove-orphans

clean: down ## Stop everything and remove build output
	@rm -rf server/target
	@rm -f docker/spark/jars/*.jar
	@echo "cleaned"
