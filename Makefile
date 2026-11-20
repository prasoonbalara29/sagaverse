# sagaverse Makefile. Run `make help` for the targets.
#
# JDK: set JAVA_HOME explicitly to your chosen Java 21 JDK, or put that path in an untracked
# .java-home file. Nothing here picks a JDK by version number (see AGENTS.md).
#
# Containers: everything talks to the Podman machine. DOCKER_HOST is pinned to its API socket so
# Testcontainers never falls back to another Docker-compatible runtime that may be installed.

SHELL := /bin/bash
.DEFAULT_GOAL := help

JAVA_HOME ?= $(shell cat .java-home 2>/dev/null)
export JAVA_HOME

PODMAN_SOCKET := $(shell podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}' 2>/dev/null)
export DOCKER_HOST ?= unix://$(PODMAN_SOCKET)
# Path of the socket *inside* the rootful Podman VM, which Ryuk mounts.
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE ?= /run/podman/podman.sock

COMPOSE := podman compose
MVN := ./mvnw -B -ntp

.PHONY: help check-java check-podman up down ps logs topics consume build test verify fmt

help: ## List targets
	@grep -hE '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*## "}{printf "  %-16s %s\n",$$1,$$2}'

check-java:
	@if [ -z "$(JAVA_HOME)" ]; then \
	  echo "JAVA_HOME is not set. export JAVA_HOME=<path-to-intended-jdk>/Contents/Home (or write it to .java-home)"; exit 1; fi
	@"$(JAVA_HOME)/bin/java" -version 2>&1 | head -1

check-podman:
	@if [ -z "$(PODMAN_SOCKET)" ]; then echo "No Podman machine found. Run: podman machine start"; exit 1; fi
	@podman info --format 'podman {{.Version.Version}} ({{.Host.MemTotal}} bytes RAM)' >/dev/null || \
	  { echo "Podman machine not reachable. Run: podman machine start"; exit 1; }

up: check-podman ## Start infra (postgres, kafka) and create topics
	$(COMPOSE) up -d --wait postgres kafka
	$(COMPOSE) run --rm topics-init

down: ## Stop and remove all containers
	$(COMPOSE) down --remove-orphans

ps: ## Show containers and health
	$(COMPOSE) ps

logs: ## Follow infra logs
	$(COMPOSE) logs -f

topics: ## List topics with partition counts
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe | grep -E '^Topic:' | awk '{print $$2, $$6, $$8}' | sort | (echo "topic partitions rf"; cat) | column -t

consume: ## Print messages on a topic: make consume TOPIC=payment.commands
	@test -n "$(TOPIC)" || { echo "usage: make consume TOPIC=<topic>"; exit 1; }
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
	  --topic $(TOPIC) --from-beginning --property print.key=true --property key.separator=' | '

build: check-java ## Install all modules locally (needed once before ./mvnw -pl <svc> spring-boot:run)
	$(MVN) install -DskipTests

test: check-java ## Unit tests only (*Test)
	$(MVN) test

verify: check-java check-podman ## Format check, Error Prone, unit + integration tests
	$(MVN) verify

fmt: check-java ## Apply formatting
	$(MVN) spotless:apply
