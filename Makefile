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

PYTHON ?= python3

expected:
	$(PYTHON) scripts/expected.py
.PHONY: expected

# corre todo de cero, espera a que terminen los clientes y compara output/ contra expected/
test:
	docker compose down
	docker compose up --build --remove-orphans --detach
	docker compose wait $$(docker compose config --services | grep '^client_')
	$(PYTHON) scripts/compare.py
.PHONY: test
