# EconScraper

**Economic data collection & AI analysis platform. 14 collectors, 13 scrapers, NLP pipeline, RAG search, and real-time health monitoring — all on a Kafka + TimescaleDB + FastAPI stack.**

![Python](https://img.shields.io/badge/Python-3.12-blue.svg)
![FastAPI](https://img.shields.io/badge/FastAPI-0.104+-green.svg)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)
![Sources](https://img.shields.io/badge/Data_Sources-27-orange.svg)

---

## What It Does

Collects economic and financial data from 27 sources (RBI, NSE, SEBI, CCIL, FRED, RSS, Reddit, Twitter, Telegram, SEC EDGAR, dark web, and more), runs NLP analysis (sentiment, NER, topic classification, threat intel), stores everything in TimescaleDB with pgvector embeddings, and serves it through a FastAPI backend with semantic search and RAG-powered Q&A.

Includes intelligent routing to two downstream dashboards — **DragonScope** (market analytics) and **LiquiFi** (Indian treasury management).

---

## Architecture

```
COLLECTION                          PROCESSING                      SERVING
─────────────                      ──────────                      ───────
14 Collectors ─┐                                                   FastAPI v4.0
  RBI DBIE     │    ┌──────────┐   ┌──────────────┐               ├─ /search/semantic
  NSE India    ├──► │  Kafka   ├──►│  NLP Worker   │──► PostgreSQL ├─ /ask (RAG)
  CCIL Rates   │    │ raw-posts│   │  - FinBERT    │    TimescaleDB├─ /trends
  FRED API     │    └──────────┘   │  - spaCy NER  │    + pgvector ├─ /digest
  SEBI         │                   │  - Embeddings  │              ├─ /data
  RSS (16)     │                   │  - Topics      │              └─ /monitoring
  data.gov.in  │                   │  - Dedup       │
  BSE          │                   └──────┬─────────┘    ┌──────────────────┐
  World Bank   │                          │              │ DragonScope      │
  IMF          │                          ▼              │  (Market View)   │
13 Scrapers ───┤                   ┌──────────────┐      ├──────────────────┤
  Reddit       │                   │   Router     │─────►│ LiquiFi          │
  Twitter      │    ┌──────────┐   │  DS / LF /   │      │  (Treasury Mgmt) │
  Telegram     ├──► │  MinIO   │   │    Both      │      └──────────────────┘
  YouTube      │    │ (raw)    │   └──────────────┘
  Discord      │    └──────────┘
  SEC EDGAR    │                   ┌──────────────┐
  Dark Web     │                   │  Health      │
  GitHub       ┘                   │  Monitor     │──► Telegram Alerts
                                   │  + AI Report │
                                   └──────────────┘
```

---

## Data Sources

### Collectors (Celery Beat — 24/7 automated)

| Source | Data | Schedule |
|--------|------|----------|
| **RBI DBIE** | Forex reserves, money supply, sectoral credit, interest rates | Daily 6 AM IST |
| **RBI Circulars** | Press releases, notifications, policy updates | Every 4h |
| **NSE Bhavcopy** | Equity prices, derivatives, FII/DII activity | Weekdays 6:30 PM |
| **BSE API** | Corporate actions, board meetings, results | Weekdays 7 PM |
| **CCIL Rates** | FBIL reference rates, yield curve, MIBOR, TREPS | Weekdays 5 PM |
| **SEBI Circulars** | Circulars, orders, press releases | Every 6h |
| **FRED API** | Fed funds rate, CPI, 10Y/2Y yields, VIX, SOFR, GDP (11 series) | Every 6h |
| **data.gov.in** | CPI, WPI, IIP, GDP, GST collections | Daily 8 AM |
| **World Bank** | GDP, inflation, current account (6 countries) | Weekly |
| **IMF** | IFS, DOT, BOP datasets | Monthly |
| **RSS Feeds** | Reuters, ET, LiveMint, Moneycontrol, CoinDesk, CNBC, FT, arXiv (16 feeds) | Every 5 min |
| **Twitter Lists** | RBI policy, SEBI, stock market, MIBOR/SOFR, treasury, Fed | Every 5 min |
| **Telegram Channels** | BloombergMarketsLive, financialjuice, WallStreetSilverOfficial | Every 10 min |

### Scrapers (on-demand or periodic)

Reddit, Twitter, Telegram, YouTube, Hacker News, Discord, Mastodon, GitHub, RSS, Generic Web, SEC EDGAR, Central Banks, Dark Web (via Tor)

---

## NLP & Analysis

| Module | What It Does |
|--------|-------------|
| **Sentiment** | FinBERT financial sentiment + VADER fallback. Hawkish/dovish policy direction. Sector-level scores (banking, markets, forex, real estate, commodities, tech) |
| **Entities** | spaCy NER + custom Indian financial entities (RBI, SEBI, NSE, BSE, CCIL, FIMMDA, FBIL, NPCI, IRDAI) and policy terms (CRR, SLR, LCR, MIBOR, TREPS, LAF, MSF) |
| **Topics** | 13-category classification: monetary_policy, fiscal_policy, inflation, employment, gdp_growth, trade_balance, banking_sector, capital_markets, crypto, real_estate, commodities, regulatory, geopolitical |
| **Financial NLP** | Ticker extraction, price mention detection, earnings sentiment, treasury relevance scoring |
| **Threat Intel** | 8 categories: data_breach, ransomware, credential_theft, financial_fraud, crypto_threat, insider_threat, supply_chain, sanctions_evasion |
| **Embeddings** | all-MiniLM-L6-v2 (384-dim) with Ollama fallback. Stored in pgvector for semantic search |
| **Daily Digest** | LLM-generated briefings via Claude or Ollama |

---

## API Endpoints

### v4.0 (current)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v4/search/semantic` | POST | pgvector cosine similarity search (384-dim) |
| `/api/v4/ask` | POST | RAG Q&A — retrieve context + LLM answer with citations |
| `/api/v4/trends` | GET | Topic and sentiment trends over time |
| `/api/v4/digest` | GET/POST | LLM-generated daily briefings |
| `/api/v4/data` | GET | Raw economic time-series queries |
| `/api/v4/monitoring/health` | GET | System health (DB, Redis, all sources, processors) |
| `/api/v4/monitoring/sources` | GET | Per-source collection stats (last 24h) |
| `/api/v4/monitoring/alerts` | GET | Recent issues and failures |

### v3.0 (legacy, still available)

`/api/scrape`, `/api/analysis`, `/api/search`, `/api/pipeline`, `/api/financial`

---

## Health Monitoring

Standalone source health checker at `monitoring/health/`. Runs independently — no database required.

```bash
# Quick reachability check (all 12 sources)
python -m monitoring.health check --quick

# Full check with structure validation against baselines
python -m monitoring.health check

# AI-powered analysis report (via Claude Opus 4.6)
python -m monitoring.health report

# Update structural baselines
python -m monitoring.health baseline

# Start scheduler daemon (quick check every 6h, deep check daily 7 AM IST)
python -m monitoring.health schedule
```

**What it monitors:** HTTP status, page structure fingerprints (table counts, CSS selectors, section headers, download URL patterns), data freshness (weekday-aware), RSS feed validity, API schema changes.

**Alerts:** Telegram notifications for broken sources. Daily AI analysis reports saved to `monitoring/health/reports/`.

---

## Quick Start

### Docker (recommended)

```bash
git clone https://github.com/beepboop2025/social-scraper.git
cd social-scraper
cp .env.example .env   # Edit with your API keys
make up                # Starts all 10 services
# API at http://localhost:8000
# Flower at http://localhost:5555
```

### Standalone (no Docker)

```bash
pip install -r requirements.txt
python -m monitoring.health check --quick   # Verify sources are reachable
python scripts/init_db.py                   # Initialize database
uvicorn api.main:app --port 8000            # Start API
```

---

## Infrastructure

| Service | Image | Port |
|---------|-------|------|
| **API** | Python 3.12 | 8000 |
| **PostgreSQL** | timescale/timescaledb-ha:pg17 | 5432 |
| **Redis** | redis:7-alpine | 6379 |
| **Kafka** | confluentinc/cp-kafka:7.7.0 | 9092 |
| **MinIO** | minio/minio | 9000 |
| **Tor Proxy** | dperson/torproxy | 9050 |
| **Celery Worker** | Python 3.12 | — |
| **Celery Beat** | Python 3.12 | — |
| **NLP Worker** | Python 3.12 | — |
| **Flower** | Python 3.12 | 5555 |

---

## Make Commands

```bash
make up          # Start all services
make down        # Stop services
make logs        # Tail all logs
make health      # System health check
make stats       # Collection statistics
make alerts      # Recent alerts
make test        # Run tests
make init        # Initialize database
make migrate     # Run Alembic migrations
make backfill    # Historical data (30 days)
make backup      # Backup to ./backups/
```

---

## Project Structure

```
├── collectors/          # 14 data source collectors (RBI, NSE, FRED, etc.)
├── scrapers/            # 13 web/social scrapers (Reddit, Twitter, etc.)
├── analysis/            # NLP modules (sentiment, NER, topics, threat intel)
├── processors/          # Pipeline processors (embeddings, dedup, digest)
├── api/routes/          # FastAPI endpoints (v3 + v4)
├── pipeline/            # Kafka producer/consumer
├── connectors/          # DragonScope + LiquiFi integrations + router
├── monitoring/          # Data quality, Telegram alerts
│   └── health/          # Source health checker + AI analysis
├── core/                # Base classes, registry, scheduler
├── storage/             # Models, raw store, vectors, TimescaleDB
├── scheduler/           # Celery Beat configuration
├── config/              # sources.yaml, alerts.yaml, processing.yaml
├── scripts/             # init_db, backfill, reprocess
├── tests/               # pytest suite
├── docker-compose.yml   # 10-service stack
└── Makefile             # Common operations
```

---

## Configuration

| File | Purpose |
|------|---------|
| `config/sources.yaml` | All 14 collectors: enabled/disabled, schedules, API keys, series IDs |
| `config/alerts.yaml` | Telegram alerts, staleness thresholds, quality rules, health checker schedule |
| `config/processing.yaml` | Embedding model, sentiment model, topic categories, digest settings |
| `.env` | API keys (FRED, Telegram, Twitter), database URLs, Redis URL |

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| **API** | FastAPI, Uvicorn, Pydantic 2 |
| **Database** | TimescaleDB (PostgreSQL 17), pgvector, Alembic |
| **Queue** | Kafka (Confluent), Celery + Redis |
| **NLP** | FinBERT (transformers), spaCy, sentence-transformers, VADER |
| **LLM** | Anthropic Claude API, Ollama (fallback) |
| **Scraping** | httpx, BeautifulSoup, trafilatura, twikit, telethon |
| **Storage** | MinIO (S3-compatible raw store), Redis (cache + pub/sub) |
| **Monitoring** | Flower, Telegram Bot, source health checker |
| **Containers** | Docker Compose (10 services) |

---

## License

MIT
