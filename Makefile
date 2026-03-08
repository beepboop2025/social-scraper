.PHONY: up down logs init backfill reprocess test lint

# ── Docker ─────────────────────────────────────────────────────
up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f --tail=100

logs-worker:
	docker compose logs -f worker

logs-api:
	docker compose logs -f api

# ── Database ───────────────────────────────────────────────────
init:
	python scripts/init_db.py

migrate:
	alembic upgrade head

# ── Data Operations ────────────────────────────────────────────
backfill:
	python scripts/backfill.py --all --days 30

backfill-source:
	python scripts/backfill.py --source $(SOURCE) --days $(DAYS)

reprocess:
	python scripts/reprocess.py --all

reprocess-reset:
	python scripts/reprocess.py --all --reset

# ── Monitoring ─────────────────────────────────────────────────
health:
	curl -s http://localhost:8000/health | python -m json.tool

stats:
	curl -s http://localhost:8000/api/v4/monitoring/stats | python -m json.tool

alerts:
	curl -s http://localhost:8000/api/v4/monitoring/alerts | python -m json.tool

flower:
	open http://localhost:5555

# ── Development ────────────────────────────────────────────────
test:
	pytest tests/ -v --tb=short

lint:
	black --check .

format:
	black .

# ── Backup ─────────────────────────────────────────────────────
backup:
	bash scripts/backup.sh ./backups
