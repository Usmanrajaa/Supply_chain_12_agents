.PHONY: help up down logs build seed test-order init clean

help:
	@echo "Targets:"
	@echo "  init        Copy .env.example -> .env if missing"
	@echo "  up          Build and start all services"
	@echo "  down        Stop and remove containers"
	@echo "  build       Rebuild images"
	@echo "  logs        Tail logs (e.g. make logs SVC=inventory)"
	@echo "  seed        Seed Neo4j demo data (Postgres seeds run automatically)"
	@echo "  test-order  Fire 4 sample orders covering all flow paths"
	@echo "  clean       Remove containers AND volumes (full reset)"

init:
	@test -f .env || cp .env.example .env

up: init
	docker compose up -d --build

down:
	docker compose down

build:
	docker compose build

logs:
	docker compose logs -f $(SVC)

seed:
	docker compose exec api python -m scripts.seed_demo_data

test-order:
	docker compose exec api python -m scripts.fire_test_order

clean:
	docker compose down -v
