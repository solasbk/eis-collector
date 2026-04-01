#!/usr/bin/env python3
"""api_server.py — EIS Investor Collector backend."""
import os
import json
import math
import io
from pathlib import Path
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from typing import Optional

import psycopg2
import psycopg2.extras

from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side, numbers


def get_db():
    database_url = os.environ.get("DATABASE_URL", "")
    if database_url:
        conn = psycopg2.connect(database_url, cursor_factory=psycopg2.extras.RealDictCursor)
    else:
        # Fallback: local SQLite-style is not available with psycopg2.
        # Raise a clear error if DATABASE_URL is not set.
        raise RuntimeError(
            "DATABASE_URL environment variable is not set. "
            "Please configure a PostgreSQL database."
        )
    return conn


def init_db(db):
    cur = db.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS investors (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            role TEXT,
            company TEXT,
            eis_company TEXT,
            sector TEXT,
            amount TEXT,
            source_url TEXT,
            source_type TEXT,
            source_name TEXT,
            context_quote TEXT,
            linkedin_url TEXT,
            date_found TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS export_log (
            id SERIAL PRIMARY KEY,
            exported_at TIMESTAMP DEFAULT NOW(),
            investor_count INTEGER DEFAULT 0,
            export_type TEXT DEFAULT 'full'
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS email_log (
            id SERIAL PRIMARY KEY,
            emailed_at TIMESTAMP DEFAULT NOW(),
            investor_count INTEGER DEFAULT 0
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_name ON investors(name)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_date ON investors(date_found)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sector ON investors(sector)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_created ON investors(created_at)")
    db.commit()
    cur.close()


def seed_db(db):
    cur = db.cursor()
    cur.execute("SELECT COUNT(*) as c FROM investors")
    row = cur.fetchone()
    count = row["c"]
    cur.close()

    if count > 0:
        return

    # Load seed data from JSON file bundled with the app
    seed_file = Path(__file__).parent / "seed_data.json"
    if seed_file.exists():
        with open(seed_file) as f:
            seed_data = json.load(f)
        cur = db.cursor()
        for inv in seed_data:
            cur.execute("""
                INSERT INTO investors (name, role, company, eis_company, sector, amount,
                source_url, source_type, source_name, context_quote, linkedin_url, date_found)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                inv.get("name"), inv.get("role"), inv.get("company"), inv.get("eis_company"),
                inv.get("sector"), inv.get("amount"), inv.get("source_url"), inv.get("source_type"),
                inv.get("source_name"), inv.get("context_quote"), inv.get("linkedin_url"), inv.get("date_found")
            ))
        db.commit()
        cur.close()
        print(f"[seed] Loaded {len(seed_data)} investors from seed_data.json")
        return

    print("[seed] No seed_data.json found. Database starts empty.")


# --- App setup ---

print(f"[startup] DATABASE_URL set: {'yes' if os.environ.get('DATABASE_URL') else 'NO'}")

db = get_db()
init_db(db)
seed_db(db)

# Log the final count after seeding
_cur = db.cursor()
_cur.execute("SELECT COUNT(*) as c FROM investors")
startup_count = _cur.fetchone()["c"]
_cur.close()
print(f"[startup] Investor count after init: {startup_count}")


@asynccontextmanager
async def lifespan(app):
    yield
    try:
        db.close()
    except Exception:
        pass


app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# --- Models ---

class InvestorCreate(BaseModel):
    name: str
    role: Optional[str] = None
    company: Optional[str] = None
    eis_company: Optional[str] = None
    sector: Optional[str] = None
    amount: Optional[str] = None
    source_url: Optional[str] = None
    source_type: Optional[str] = None
    source_name: Optional[str] = None
    context_quote: Optional[str] = None
    linkedin_url: Optional[str] = None
    date_found: Optional[str] = None


class BatchInvestors(BaseModel):
    investors: list[InvestorCreate]


# --- Helpers ---

def row_to_dict(row):
    if row is None:
        return None
    return dict(row)


def rows_to_list(rows):
    return [dict(r) for r in rows]


# --- Endpoints ---

@app.get("/api/investors")
def list_investors(
    search: Optional[str] = Query(None),
    origin: Optional[str] = Query(None),
    source_type: Optional[str] = Query(None),
    source_name: Optional[str] = Query(None),
    sector: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=100),
    sort_by: str = Query("date_found"),
    sort_dir: str = Query("desc"),
):
    conditions = []
    params = []

    if search:
        conditions.append("(name ILIKE %s OR company ILIKE %s OR eis_company ILIKE %s OR role ILIKE %s)")
        s = f"%{search}%"
        params.extend([s, s, s, s])

    if origin == "ch":
        conditions.append("source_name = 'Companies House'")
    elif origin == "web":
        conditions.append("(source_name IS NULL OR source_name != 'Companies House')")

    if source_type:
        conditions.append("source_type = %s")
        params.append(source_type)

    if source_name:
        conditions.append("source_name = %s")
        params.append(source_name)

    if sector:
        conditions.append("sector = %s")
        params.append(sector)

    if date_from:
        conditions.append("date_found >= %s")
        params.append(date_from)

    if date_to:
        conditions.append("date_found <= %s")
        params.append(date_to)

    where = " AND ".join(conditions) if conditions else "1=1"

    # Validate sort
    allowed_sort = {"date_found", "name", "eis_company", "sector", "amount", "created_at"}
    if sort_by not in allowed_sort:
        sort_by = "date_found"
    if sort_dir not in ("asc", "desc"):
        sort_dir = "desc"

    cur = db.cursor()

    # Count
    count_sql = f"SELECT COUNT(*) as total FROM investors WHERE {where}"
    cur.execute(count_sql, params)
    total = cur.fetchone()["total"]

    # Fetch
    offset = (page - 1) * per_page
    data_sql = f"SELECT * FROM investors WHERE {where} ORDER BY {sort_by} {sort_dir} LIMIT %s OFFSET %s"
    cur.execute(data_sql, params + [per_page, offset])
    rows = cur.fetchall()
    cur.close()

    return {
        "investors": rows_to_list(rows),
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": math.ceil(total / per_page) if total > 0 else 1,
    }


@app.get("/api/investors/{investor_id}")
def get_investor(investor_id: int):
    cur = db.cursor()
    cur.execute("SELECT * FROM investors WHERE id = %s", [investor_id])
    row = cur.fetchone()
    cur.close()
    if not row:
        raise HTTPException(status_code=404, detail="Investor not found")
    return row_to_dict(row)


@app.get("/api/stats")
def get_stats():
    cur = db.cursor()

    cur.execute("SELECT COUNT(*) as c FROM investors")
    total = cur.fetchone()["c"]

    # New this week (last 7 days)
    week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    cur.execute(
        "SELECT COUNT(*) as c FROM investors WHERE date_found >= %s", [week_ago]
    )
    new_this_week = cur.fetchone()["c"]

    # Top sector
    cur.execute(
        "SELECT sector, COUNT(*) as c FROM investors GROUP BY sector ORDER BY c DESC LIMIT 1"
    )
    top_sector_row = cur.fetchone()
    top_sector = top_sector_row["sector"] if top_sector_row else "N/A"

    # Source types scanned
    cur.execute("SELECT COUNT(DISTINCT source_type) as c FROM investors")
    sources = cur.fetchone()["c"]

    # Unique sectors list
    cur.execute("SELECT DISTINCT sector FROM investors ORDER BY sector")
    sectors = [r["sector"] for r in cur.fetchall()]

    # Source types list
    cur.execute("SELECT DISTINCT source_type FROM investors ORDER BY source_type")
    source_types = [r["source_type"] for r in cur.fetchall()]

    # Source names list
    cur.execute(
        "SELECT DISTINCT source_name FROM investors WHERE source_name IS NOT NULL AND source_name != '' ORDER BY source_name"
    )
    source_names = [r["source_name"] for r in cur.fetchall()]

    cur.close()

    return {
        "total_investors": total,
        "new_this_week": new_this_week,
        "top_sector": top_sector,
        "sources_scanned": sources,
        "sectors": sectors,
        "source_types": source_types,
        "source_names": source_names,
    }


@app.post("/api/scan")
def trigger_scan():
    from scanner import run_scan, get_scan_status
    status = get_scan_status()
    if status["running"]:
        return {"status": "already_running", "message": "A scan is already in progress."}
    started = run_scan()
    if started:
        return {"status": "started", "message": "Scan started. Poll /api/scan/status for progress."}
    return {"status": "error", "message": "Failed to start scan."}


@app.get("/api/scan/status")
def scan_status():
    from scanner import get_scan_status
    return get_scan_status()


@app.post("/api/ch-scan")
def trigger_ch_scan():
    from ch_scanner import run_ch_scan, get_ch_scan_status
    status = get_ch_scan_status()
    if status["running"]:
        return {"status": "already_running", "message": "A Companies House scan is already in progress."}
    started = run_ch_scan()
    if started:
        return {"status": "started", "message": "Companies House scan started. Poll /api/ch-scan/status for progress."}
    return {"status": "error", "message": "Failed to start Companies House scan."}


@app.get("/api/ch-scan/status")
def ch_scan_status():
    from ch_scanner import get_ch_scan_status
    return get_ch_scan_status()


# Keep legacy endpoint for backward compatibility
@app.post("/api/collect")
def trigger_collection():
    from scanner import run_scan, get_scan_status
    status = get_scan_status()
    if status["running"]:
        return {"status": "already_running", "message": "A scan is already in progress."}
    run_scan()
    return {"status": "started", "message": "Scan started."}


@app.post("/api/investors/batch", status_code=201)
def batch_upsert(batch: BatchInvestors):
    inserted = 0
    skipped = 0
    cur = db.cursor()

    for inv in batch.investors:
        # Check for duplicate by name + eis_company
        cur.execute(
            "SELECT id FROM investors WHERE name = %s AND eis_company = %s",
            [inv.name, inv.eis_company]
        )
        existing = cur.fetchone()

        if existing:
            # Update existing record
            cur.execute("""
                UPDATE investors SET role=%s, company=%s, sector=%s, amount=%s,
                source_url=%s, source_type=%s, source_name=%s, context_quote=%s,
                linkedin_url=%s, date_found=%s
                WHERE id=%s
            """, (
                inv.role, inv.company, inv.sector, inv.amount,
                inv.source_url, inv.source_type, inv.source_name, inv.context_quote,
                inv.linkedin_url, inv.date_found, existing["id"]
            ))
            skipped += 1
        else:
            cur.execute("""
                INSERT INTO investors (name, role, company, eis_company, sector, amount,
                source_url, source_type, source_name, context_quote, linkedin_url, date_found)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                inv.name, inv.role, inv.company, inv.eis_company, inv.sector, inv.amount,
                inv.source_url, inv.source_type, inv.source_name, inv.context_quote,
                inv.linkedin_url, inv.date_found
            ))
            inserted += 1

    db.commit()
    cur.close()
    return {"inserted": inserted, "updated": skipped, "total": len(batch.investors)}


@app.get("/api/export/last")
def get_last_export():
    """Return info about the last 'new' export."""
    cur = db.cursor()
    cur.execute(
        "SELECT exported_at, investor_count FROM export_log WHERE export_type = 'new' ORDER BY id DESC LIMIT 1"
    )
    row = cur.fetchone()
    if row:
        # Count investors added since that export
        cur.execute(
            "SELECT COUNT(*) as c FROM investors WHERE created_at > %s", [row["exported_at"]]
        )
        new_since = cur.fetchone()["c"]
        cur.close()
        return {
            "last_exported_at": row["exported_at"],
            "last_export_count": row["investor_count"],
            "new_since_last_export": new_since,
        }
    else:
        cur.execute("SELECT COUNT(*) as c FROM investors")
        total = cur.fetchone()["c"]
        cur.close()
        return {
            "last_exported_at": None,
            "last_export_count": 0,
            "new_since_last_export": total,
        }


def build_excel(investors, title_text, subtitle_text):
    """Build a formatted Excel workbook from a list of investor dicts."""
    wb = Workbook()
    ws = wb.active
    ws.title = "EIS Investors"

    # ── Colours ──
    HEADER_FILL = PatternFill(start_color="1B3A4B", end_color="1B3A4B", fill_type="solid")
    HEADER_FONT = Font(name="Calibri", size=11, bold=True, color="FFFFFF")
    EVEN_FILL = PatternFill(start_color="F5F7FA", end_color="F5F7FA", fill_type="solid")
    ODD_FILL = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid")
    BODY_FONT = Font(name="Calibri", size=10, color="1A1A1A")
    LINK_FONT = Font(name="Calibri", size=10, color="2980B9", underline="single")
    MUTED_FONT = Font(name="Calibri", size=10, color="808080", italic=True)
    AMOUNT_FONT = Font(name="Calibri", size=10, bold=True, color="1A6B3C")
    THIN_BORDER = Border(bottom=Side(style="thin", color="E0E0E0"))
    HEADER_BORDER = Border(bottom=Side(style="medium", color="0F2B3A"))

    # ── Title row ──
    ws.merge_cells("A1:K1")
    title_cell = ws["A1"]
    title_cell.value = title_text
    title_cell.font = Font(name="Calibri", size=16, bold=True, color="1B3A4B")
    title_cell.alignment = Alignment(vertical="center")
    ws.row_dimensions[1].height = 36

    # ── Subtitle row ──
    ws.merge_cells("A2:K2")
    sub_cell = ws["A2"]
    sub_cell.value = subtitle_text
    sub_cell.font = Font(name="Calibri", size=10, color="666666")
    sub_cell.alignment = Alignment(vertical="center")
    ws.row_dimensions[2].height = 22

    # ── Spacer row ──
    ws.row_dimensions[3].height = 8

    # ── Headers (row 4) ──
    headers = [
        ("Name", 26),
        ("Role", 24),
        ("Company", 24),
        ("EIS Company", 28),
        ("Sector", 26),
        ("Amount", 16),
        ("Source", 20),
        ("Source Type", 14),
        ("Date Found", 14),
        ("LinkedIn", 32),
        ("Source URL", 40),
    ]

    header_row = 4
    for col_idx, (header_name, col_width) in enumerate(headers, 1):
        cell = ws.cell(row=header_row, column=col_idx, value=header_name)
        cell.font = HEADER_FONT
        cell.fill = HEADER_FILL
        cell.alignment = Alignment(horizontal="left", vertical="center")
        cell.border = HEADER_BORDER
        ws.column_dimensions[cell.column_letter].width = col_width

    ws.row_dimensions[header_row].height = 30

    # ── Data rows ──
    for row_idx, inv in enumerate(investors, header_row + 1):
        fill = EVEN_FILL if (row_idx - header_row) % 2 == 0 else ODD_FILL

        values = [
            inv.get("name", ""),
            inv.get("role", ""),
            inv.get("company", ""),
            inv.get("eis_company", ""),
            inv.get("sector", ""),
            inv.get("amount", ""),
            inv.get("source_name", ""),
            inv.get("source_type", ""),
            inv.get("date_found", ""),
            inv.get("linkedin_url", "") or "",
            inv.get("source_url", "") or "",
        ]

        for col_idx, val in enumerate(values, 1):
            cell = ws.cell(row=row_idx, column=col_idx, value=val)
            cell.fill = fill
            cell.border = THIN_BORDER
            cell.alignment = Alignment(vertical="center", wrap_text=False)

            if col_idx == 1:
                cell.font = Font(name="Calibri", size=10, bold=True, color="1A1A1A")
            elif col_idx == 6:
                if val and val not in ("Undisclosed", "undisclosed", "Not disclosed", "not disclosed", ""):
                    cell.font = AMOUNT_FONT
                else:
                    cell.font = MUTED_FONT
                    cell.value = "Undisclosed"
            elif col_idx == 10 and val:
                cell.font = LINK_FONT
                cell.hyperlink = val
                cell.value = val
            elif col_idx == 11 and val:
                cell.font = LINK_FONT
                cell.hyperlink = val
                cell.value = val
            else:
                cell.font = BODY_FONT

        ws.row_dimensions[row_idx].height = 24

    # ── Freeze panes ──
    ws.freeze_panes = f"A{header_row + 1}"

    # ── Auto-filter ──
    last_row = header_row + len(investors)
    last_col_letter = ws.cell(row=header_row, column=len(headers)).column_letter
    ws.auto_filter.ref = f"A{header_row}:{last_col_letter}{last_row}"

    # ── Print settings ──
    ws.sheet_properties.pageSetUpPr = None
    ws.page_setup.orientation = "landscape"
    ws.page_setup.fitToWidth = 1
    ws.page_setup.fitToHeight = 0

    buffer = io.BytesIO()
    wb.save(buffer)
    buffer.seek(0)
    return buffer


@app.get("/api/export/excel")
def export_excel(
    search: Optional[str] = Query(None),
    source_type: Optional[str] = Query(None),
    sector: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    sort_by: str = Query("date_found"),
    sort_dir: str = Query("desc"),
):
    """Export all matching investors as a formatted Excel workbook."""
    conditions = []
    params = []

    if search:
        conditions.append("(name ILIKE %s OR company ILIKE %s OR eis_company ILIKE %s OR role ILIKE %s)")
        s = f"%{search}%"
        params.extend([s, s, s, s])
    if source_type:
        conditions.append("source_type = %s")
        params.append(source_type)
    if sector:
        conditions.append("sector = %s")
        params.append(sector)
    if date_from:
        conditions.append("date_found >= %s")
        params.append(date_from)
    if date_to:
        conditions.append("date_found <= %s")
        params.append(date_to)

    where = " AND ".join(conditions) if conditions else "1=1"
    allowed_sort = {"date_found", "name", "eis_company", "sector", "amount", "created_at"}
    if sort_by not in allowed_sort:
        sort_by = "date_found"
    if sort_dir not in ("asc", "desc"):
        sort_dir = "desc"

    cur = db.cursor()
    cur.execute(
        f"SELECT * FROM investors WHERE {where} ORDER BY {sort_by} {sort_dir}", params
    )
    rows = cur.fetchall()
    cur.close()
    investors = [dict(r) for r in rows]

    now_str = datetime.now().strftime('%d %B %Y at %H:%M')
    buffer = build_excel(
        investors,
        title_text="EIS Investor Collector \u2014 Full Export",
        subtitle_text=f"Generated {now_str}  \u00b7  {len(investors)} investors",
    )

    filename = f"eis_investors_{datetime.now().strftime('%Y-%m-%d')}.xlsx"
    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/export/excel-new")
def export_excel_new():
    """Export only investors added since the last 'new' export, then record this export."""
    cur = db.cursor()

    # Find last export timestamp
    cur.execute(
        "SELECT exported_at FROM export_log WHERE export_type = 'new' ORDER BY id DESC LIMIT 1"
    )
    last_row = cur.fetchone()
    last_exported_at = last_row["exported_at"] if last_row else None

    if last_exported_at:
        cur.execute(
            "SELECT * FROM investors WHERE created_at > %s ORDER BY created_at DESC",
            [last_exported_at],
        )
    else:
        cur.execute("SELECT * FROM investors ORDER BY created_at DESC")

    rows = cur.fetchall()
    investors = [dict(r) for r in rows]

    if len(investors) == 0:
        cur.close()
        raise HTTPException(status_code=404, detail="No new investors since last export.")

    now = datetime.now()
    now_str = now.strftime('%d %B %Y at %H:%M')
    since_str = last_exported_at.strftime("%Y-%m-%d %H:%M") if last_exported_at else "the beginning"
    buffer = build_excel(
        investors,
        title_text="EIS Investor Collector \u2014 New Since Last Export",
        subtitle_text=f"Generated {now_str}  \u00b7  {len(investors)} new investors since {since_str}",
    )

    # Record this export
    cur.execute(
        "INSERT INTO export_log (exported_at, investor_count, export_type) VALUES (%s, %s, 'new')",
        [now, len(investors)],
    )
    db.commit()
    cur.close()

    filename = f"eis_investors_new_{now.strftime('%Y-%m-%d')}.xlsx"
    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/email/new-investors")
def get_new_investors_for_email():
    """Return investors added since last email, and record the email event."""
    cur = db.cursor()
    cur.execute(
        "SELECT emailed_at FROM email_log ORDER BY id DESC LIMIT 1"
    )
    last_row = cur.fetchone()
    last_emailed_at = last_row["emailed_at"] if last_row else None

    if last_emailed_at:
        cur.execute(
            "SELECT name, role, company, eis_company, sector, amount, source_name, date_found "
            "FROM investors WHERE created_at > %s ORDER BY created_at DESC",
            [last_emailed_at],
        )
    else:
        cur.execute(
            "SELECT name, role, company, eis_company, sector, amount, source_name, date_found "
            "FROM investors ORDER BY created_at DESC"
        )

    rows = cur.fetchall()
    cur.close()
    investors = [dict(r) for r in rows]
    return {
        "investors": investors,
        "count": len(investors),
        "since": last_emailed_at,
    }


@app.post("/api/email/mark-sent")
def mark_email_sent(count: int = Query(0)):
    """Record that an email digest was sent."""
    now = datetime.now()
    cur = db.cursor()
    cur.execute(
        "INSERT INTO email_log (emailed_at, investor_count) VALUES (%s, %s)",
        [now, count],
    )
    db.commit()
    cur.close()
    return {"status": "recorded", "emailed_at": now.strftime("%Y-%m-%dT%H:%M:%S"), "count": count}


# --- Serve static frontend ---
STATIC_DIR = Path(__file__).parent / "static"
if STATIC_DIR.is_dir():
    @app.get("/")
    async def serve_index():
        return FileResponse(STATIC_DIR / "index.html")

    app.mount("/", StaticFiles(directory=str(STATIC_DIR)), name="static")


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
