.PHONY: up down logs psql redis-cli install test lint typecheck fmt migrate ingestor api

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

psql:
	docker compose exec postgres psql -U sports -d sports_platform

redis-cli:
	docker compose exec redis redis-cli

install:
	uv venv
	uv pip install -e ".[dev]"

migrate:
	psql $$DATABASE_URL -f migrations/001_initial.sql

test:
	pytest -v

lint:
	ruff check .

typecheck:
	mypy schemas services

fmt:
	ruff format .
	ruff check --fix .

ingestor:
	python -m services.mlb_ingestor

api:
	python -m services.query_api
