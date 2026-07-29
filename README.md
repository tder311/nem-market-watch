# NEM Market Watch

A React + FastAPI app for monitoring Australia's National Electricity Market
(NEM): live and historical prices, generation mix, system-adequacy forecasts,
and bid analysis across NSW, VIC, QLD, SA and TAS.

This repo was extracted from the NemDash monorepo to separate the
market-watching app from continuous NEMWEB ingestion.

## Architecture

**Stack**: FastAPI backend (`backend/`) + React frontend (`frontend/`), both
reading a shared PostgreSQL database.

This app **never writes** to Postgres. The [`nem-ingestion`](../nem-ingestion)
service is the sole writer — it owns the schema (all `CREATE TABLE` DDL) and
polls NEMWEB continuously to keep the database current. `nem-market-watch`
only opens a connection pool and runs read queries against the tables
`nem-ingestion` fills.

```
┌────────────────────────┐        ┌──────────────────────────────┐
│      nem-ingestion      │        │        nem-market-watch       │
│  (separate repo, sole   │        │   (this repo) FastAPI + React │
│   writer + schema owner)│        │        http://localhost:8000  │
└───────────┬──────────────┘        └───────────────┬────────────────┘
            │ writes                                 │ reads only
            ▼                                        ▼
┌─────────────────────────────────────────────────────────────────┐
│                       PostgreSQL Database                       │
└─────────────────────────────────────────────────────────────────┘
```

## Pages

- **Dashboard** — live prices + interactive region map
- **Market Metrics** — TB spreads, capture rates/prices, price-setter frequency
- **Ask NemDash** — conversational chat over live NEM data (OpenAI, tool-calling)
- **Bid Analysis** — price/quantity bid bands by DUID and date
- **Downloads** — CSV export of prices, generation, PASA, and daily metrics
- **State drilldown** — per-region price history, fuel mix, and generation
- **Database health** — record counts, date ranges, and gap detection

## Local development

Prerequisites: Python 3.8+, Node.js, and a PostgreSQL instance (or Docker to
run one via `docker-compose`).

```bash
make install       # installs backend + frontend deps
make dev           # starts Postgres (if Docker is available), backend, and frontend
```

`make dev` runs `./dev.sh`, which starts **only the backend and frontend** —
there is no ingestion worker in this repo. Without `nem-ingestion`'s worker
running against the same `DATABASE_URL`, the dashboard will come up cleanly
but show no live data (empty tables, not errors). To see live data locally,
also run the `nem-ingestion` worker pointed at the same database.

Individual targets: `make run-backend`, `make run-frontend`, `make db` /
`make db-stop`. Run `make help` for the full list.

## Environment variables

Set in `backend/.env` (copy from `backend/.env.example`):

| Variable | Purpose |
|---|---|
| `DATABASE_URL` | PostgreSQL connection string (required) |
| `ALLOWED_ORIGINS` | Comma-separated CORS origins (default `http://localhost:3000,http://localhost:8050`) |
| `OPENAI_API_KEY` | Required for the Ask NemDash chat tab |
| `OPENAI_MODEL` | Optional, defaults to `gpt-4o` |
| `OPENAI_BASE_URL` | Optional, for Azure/proxy/compatible endpoints |
| `HOST`, `PORT`, `RELOAD`, `LOG_LEVEL` | Uvicorn server config (`run.py`) |

The frontend reads `REACT_APP_API_URL` (optional; defaults to same-origin
`/api` via the CRA dev proxy in `frontend/package.json`).

## Tests

**Backend** (from `backend/`):

```bash
python -m pytest tests/unit -x -q
```

Requires `DATABASE_URL` pointing at a **disposable** test database — the
`test_db` fixture applies a tests-only schema mirror (`tests/schema.sql`,
kept in sync with `nem-ingestion`'s real DDL) and **TRUNCATEs every table it
seeds before each test**. Never export a live/shared `DATABASE_URL` before
running pytest. `make db` starts a local Postgres via `docker-compose.yml`
if you don't have one.

**Frontend** (from `frontend/`):

```bash
npm install
CI=true npm test -- --watchAll=false
```

## Repo layout

- `backend/app/main.py` — FastAPI app: read-only REST endpoints (prices,
  dispatch, generation, regions, metrics, PASA, exports, bids, chat)
- `backend/app/database.py` — connection pool + read queries only (no DDL,
  no inserts — see the architecture note above)
- `backend/app/agent.py` — the Ask NemDash tool-calling agent (read-only tools)
- `backend/app/models.py` — Pydantic response models
- `backend/tests/schema.sql` — test-only schema mirror for the pytest fixtures
- `frontend/` — the React SPA (Create React App)
