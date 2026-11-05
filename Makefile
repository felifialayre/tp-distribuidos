up:
	docker compose up --build --remove-orphans --detach
	docker compose logs --follow
.PHONY: up

down:
	docker compose stop -t 5
	docker compose down
.PHONY: down

logs:
	docker compose logs
.PHONY: logs

# genera docker-compose.yaml con las replicas de cada nodo de nodes.yaml
compose:
	cd scripts/compose_generator && go run . ../../nodes.yaml ../../docker-compose.yaml
.PHONY: compose

PYTHON ?= python3

EXPECTED_PYTHON = .venv-test/bin/python

$(EXPECTED_PYTHON):
	python3 -m venv .venv-test
	.venv-test/bin/pip install -q -r scripts/requirements.txt

expected: $(EXPECTED_PYTHON)
	$(EXPECTED_PYTHON) scripts/expected.py
.PHONY: expected

# corre todo de cero, espera a que terminen los clientes y compara output/ contra expected/
test:
	docker compose down
	docker compose up --build --remove-orphans --detach
	docker compose wait $$(docker compose config --services | grep '^client_')
	$(PYTHON) scripts/compare.py
.PHONY: test
