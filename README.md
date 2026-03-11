# Social Scraper Intelligence Platform v3.0

**Real-time financial intelligence aggregation across 15 data sources with NLP analysis, threat detection, and automated routing to downstream analytics dashboards.**

![Python](https://img.shields.io/badge/Python-3.12-blue.svg)
![FastAPI](https://img.shields.io/badge/FastAPI-0.104+-green.svg)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)
![Sources](https://img.shields.io/badge/Data_Sources-15-orange.svg)
![License](https://img.shields.io/badge/License-MIT-yellow.svg)

---

## Features

### Data Sources (15)

- **Social platforms** -- Twitter, Reddit, Telegram, Discord, YouTube, Mastodon, Hacker News
- **Financial feeds** -- SEC EDGAR filings, Central Bank publications, RSS aggregation (16 feeds)
- **Developer intelligence** -- GitHub repository and release tracking
- **Dark web** -- Tor SOCKS5 proxy for threat intel, IOC extraction across 8 threat categories
- **General web** -- Configurable generic web scraper with article extraction

### Financial NLP

- **Sentiment analysis** -- FinBERT financial sentiment with VADER fallback; hawkish/dovish policy direction scoring
- **Entity recognition** -- spaCy NER extended with Indian financial entities (RBI, SEBI, NSE, CCIL, FIMMDA, FBIL) and policy terms (CRR, SLR, MIBOR, TREPS, LAF, MSF)
- **Topic classification** -- 13 categories including monetary policy, capital markets, crypto, commodities, and geopolitical
- **Ticker extraction** -- Automatic ticker detection, price mention parsing, earnings sentiment, and treasury relevance scoring
- **Threat intelligence** -- Classification across data breach, ransomware, credential theft, financial fraud, crypto threat, insider threat, supply chain, and sanctions evasion
- **Embeddings** -- all-MiniLM-L6-v2 (384-dim) stored in pgvector for semantic search, with Ollama fallback

### Connectors

- **DragonScope** -- Market analytics dashboard integration via Redis pub/sub and REST API push
- **LiquiFi** -- Indian treasury management dashboard with filtered content delivery
- **Smart Router** -- Classifies each piece of content and routes to DragonScope, LiquiFi, or both based on relevance scoring

### Infrastructure

- **Celery Beat scheduler** -- 24/7 automated collection with tiered frequencies (5 min to monthly)
- **Kafka pipeline** -- Decoupled ingestion and processing via topic-based message streaming
- **Health monitoring** -- Source reachability checks, structural fingerprinting, data freshness tracking, and Telegram alerting
- **AI-generated digests** -- Daily briefings via Claude or Ollama with citation-backed RAG Q&A

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| API | FastAPI, Uvicorn, Pydantic v2 |
| Database | TimescaleDB (PostgreSQL 16), pgvector, Alembic |
| Queue | Apache Kafka (Confluent), Celery + Redis |
| NLP | FinBERT (transformers), spaCy, sentence-transformers, VADER |
| LLM | Anthropic Claude API, Ollama (fallback) |
| Scraping | httpx, BeautifulSoup, trafilatura, twikit, telethon |
| Object Storage | MinIO (S3-compatible) |
| Dark Web | Tor SOCKS5 proxy (dperson/torproxy) |
| Monitoring | Flower (Celery), Telegram Bot alerts |
| Containers | Docker Compose (11 services) |

---

## Getting Started

### Prerequisites

- Docker and Docker Compose
- API keys for desired data sources (see `.env.example`)

### Setup

```bash
git clone https://github.com/beepboop2025/social-scraper.git
cd social-scraper
cp .env.example .env    # Add your API keys and database password
docker compose up -d    # Starts all services
```

The API will be available at `http://localhost:8000` and Flower (Celery monitoring) at `http://localhost:5555`.

### Standalone (without Docker)

```bash
pip install -r requirements.txt
python scripts/init_db.py
uvicorn api.main:app --port 8000
```

### Common Operations

```bash
make up          # Start all services
make down        # Stop all services
make logs        # Tail logs across services
make health      # Run system health check
make test        # Run test suite
make init        # Initialize database schema
make migrate     # Run Alembic migrations
make backfill    # Backfill 30 days of historical data
make backup      # Backup database to ./backups/
```

---

## Architecture

```
DATA SOURCES                   PIPELINE                        SERVING
────────────                   ────────                        ───────
Twitter     ─┐                                                 FastAPI
Reddit      │                                                  ├─ /search/semantic
Telegram    │   ┌──────────┐   ┌───────────────┐               ├─ /ask (RAG)
Discord     │   │          │   │  NLP Workers   │               ├─ /trends
YouTube     ├──>│  Kafka   ├──>│  - FinBERT     │──> PostgreSQL ├─ /digest
Mastodon    │   │          │   │  - spaCy NER   │    TimescaleDB├─ /data
GitHub      │   └──────────┘   │  - Embeddings  │    + pgvector └─ /monitoring
SEC EDGAR   │                  │  - Topics      │
Central Banks│                  └───────┬───────┘
Hacker News │                          │
RSS Feeds   │                          v
Dark Web    │                  ┌───────────────┐   ┌────────────────┐
Generic Web ─┘                  │    Router     │──>│  DragonScope   │
                               │  DS / LF /    │   │  (Market View) │
      ┌──────────┐             │    Both       │   ├────────────────┤
      │  MinIO   │             └───────────────┘   │  LiquiFi       │
      │  (raw)   │                                 │  (Treasury)    │
      └──────────┘             ┌───────────────┐   └────────────────┘
                               │  Health       │
                               │  Monitor      │──> Telegram Alerts
                               └───────────────┘
```

Data flows through three stages. **Collection**: 15 scrapers and collectors pull from social platforms, financial APIs, RSS feeds, and dark web sources on configurable schedules managed by Celery Beat. Raw content is published to Kafka topics and archived in MinIO. **Processing**: dedicated NLP workers consume from Kafka, running sentiment analysis, entity extraction, topic classification, threat detection, and embedding generation. Processed records are stored in TimescaleDB with pgvector indexes. **Routing**: the smart router evaluates each record's financial relevance and forwards it to DragonScope (market analytics), LiquiFi (treasury management), or both via Redis pub/sub and REST API calls.

---

## Project Structure

```
social_scraper/
├── scrapers/            # 15 data source scrapers
├── collectors/          # Automated data collectors (Celery tasks)
├── analysis/            # NLP modules (sentiment, NER, topics, threat intel)
├── processors/          # Pipeline processors (embeddings, dedup, digest)
├── connectors/          # DragonScope + LiquiFi integrations + router
├── pipeline/            # Kafka producer/consumer
├── api/                 # FastAPI application and route modules
├── core/                # Base classes, registry, scheduler
├── storage/             # Models, raw store, vectors, TimescaleDB
├── scheduler/           # Celery Beat configuration
├── monitoring/          # Data quality checks, health monitor, Telegram alerts
├── config/              # sources.yaml, alerts.yaml, processing.yaml
├── scripts/             # Database init, backfill, reprocessing utilities
├── tests/               # pytest suite
├── docker-compose.yml   # Full service stack
├── Dockerfile
├── Makefile
└── requirements.txt
```

---

## License

MIT
