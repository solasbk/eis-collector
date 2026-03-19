# EIS Investor Collector

A web application that searches for and tracks individuals who invest in UK EIS (Enterprise Investment Scheme) qualifying companies.

## Features

- **Investor database** — 380+ EIS/SEIS investor records with name, role, company, EIS company, sector, amount, and source
- **On-demand web scan** — Click "Run Collection" to search the web for new EIS investor references
- **Search and filter** — Full-text search, filter by source type, sector, date range
- **Excel export** — Formatted .xlsx export with all investors or just new additions since last export
- **Dark/light mode** — Toggle between themes

## Tech Stack

- **Backend**: Python / FastAPI / SQLite
- **Frontend**: Vanilla HTML/CSS/JS (no build step)
- **Scanner**: DuckDuckGo + Bing search with Anthropic LLM extraction

## Deploy to Render

1. Fork this repo
2. Go to [render.com](https://render.com) and create a new **Web Service**
3. Connect your GitHub repo
4. Render will auto-detect the `render.yaml` config
5. Optionally set `ANTHROPIC_API_KEY` in Environment to enable LLM-powered scan extraction
6. Deploy

The free tier will spin down after 15 minutes of inactivity (first request takes ~30s to wake up).

**Important**: The `render.yaml` includes a persistent disk for the SQLite database. On the free plan, you can use Render without a disk — the database will reset on each deploy but will persist between requests.

## Run Locally

```bash
pip install -r requirements.txt
python api_server.py
```

Open http://localhost:8000 in your browser.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/investors` | Paginated, searchable investor list |
| GET | `/api/investors/{id}` | Single investor detail |
| GET | `/api/stats` | Dashboard statistics |
| POST | `/api/scan` | Trigger a web scan for new investors |
| GET | `/api/scan/status` | Poll scan progress |
| POST | `/api/investors/batch` | Batch upsert investors (deduplicates) |
| GET | `/api/export/excel` | Download full Excel export |
| GET | `/api/export/excel-new` | Download new investors since last export |
| GET | `/api/email/new-investors` | Get investors since last email digest |
| POST | `/api/email/mark-sent` | Record email digest sent |
