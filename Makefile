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