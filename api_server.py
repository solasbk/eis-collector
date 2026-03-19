#!/usr/bin/env python3
"""api_server.py — EIS Investor Collector backend."""
import os
import sqlite3
import json
import math
import io
from pathlib import Path
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side, numbers

# Use /opt/render/project/data for persistent disk on Render, else local
DATA_DIR = os.environ.get("DATA_DIR", ".")
DB_PATH = os.path.join(DATA_DIR, "eis_investors.db")


def get_db():
    db = sqlite3.connect(DB_PATH, check_same_thread=False)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    return db


def init_db(db):
    db.executescript("""
        CREATE TABLE IF NOT EXISTS investors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS export_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            exported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            investor_count INTEGER DEFAULT 0,
            export_type TEXT DEFAULT 'full'
        );
        CREATE TABLE IF NOT EXISTS email_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            emailed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            investor_count INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_name ON investors(name);
        CREATE INDEX IF NOT EXISTS idx_date ON investors(date_found);
        CREATE INDEX IF NOT EXISTS idx_sector ON investors(sector);
        CREATE INDEX IF NOT EXISTS idx_created ON investors(created_at);
    """)
    db.commit()


def seed_db(db):
    count = db.execute("SELECT COUNT(*) as c FROM investors").fetchone()["c"]
    if count > 0:
        return

    # Load seed data from JSON file
    seed_file = Path(__file__).parent / "seed_data.json"
    if seed_file.exists():
        with open(seed_file) as f:
            seed_data = json.load(f)
        for inv in seed_data:
            db.execute("""
                INSERT INTO investors (name, role, company, eis_company, sector, amount,
                source_url, source_type, source_name, context_quote, linkedin_url, date_found)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                inv.get("name"), inv.get("role"), inv.get("company"), inv.get("eis_company"),
                inv.get("sector"), inv.get("amount"), inv.get("source_url"), inv.get("source_type"),
                inv.get("source_name"), inv.get("context_quote"), inv.get("linkedin_url"), inv.get("date_found")
            ))
        db.commit()
        return

    # Fallback: minimal sample data
    sample_data = [
        {
            "name": "James Whitfield",
            "role": "Managing Director",
            "company": "Beacon Capital Partners",
            "eis_company": "Revolut",
            "sector": "Fintech",
            "amount": "£150,000",
            "source_url": "https://www.techcrunch.com/example",
            "source_type": "News",
            "source_name": "TechCrunch",
            "context_quote": "James Whitfield, Managing Director at Beacon Capital Partners, confirmed his personal EIS-qualifying investment into Revolut's latest round.",
            "linkedin_url": "https://linkedin.com/in/example",
            "date_found": "2026-02-28"
        },
        {
            "name": "Sarah Ellingham",
            "role": "Partner",
            "company": "Venture Associates LLP",
            "eis_company": "Graphcore",
            "sector": "AI / Semiconductors",
            "amount": "Undisclosed",
            "source_url": "https://www.ft.com/example",
            "source_type": "News",
            "source_name": "Financial Times",
            "context_quote": "Sarah Ellingham of Venture Associates has backed Graphcore through the Enterprise Investment Scheme, sources close to the deal confirmed.",
            "linkedin_url": "https://linkedin.com/in/example",
            "date_found": "2026-02-22"
        },
        {
            "name": "Marcus Chen",
            "role": "Angel Investor",
            "company": "Independent",
            "eis_company": "Bulb Energy",
            "sector": "Clean Energy",
            "amount": "£75,000",
            "source_url": "https://www.cityam.com/example",
            "source_type": "News",
            "source_name": "City A.M.",
            "context_quote": "Angel investor Marcus Chen made a £75,000 EIS investment into Bulb Energy during their Series B extension.",
            "linkedin_url": None,
            "date_found": "2026-02-15"
        },
        {
            "name": "Victoria Hartley",
            "role": "CEO",
            "company": "Hartley Wealth Management",
            "eis_company": "Brewdog",
            "sector": "Food & Beverage",
            "amount": "£200,000",
            "source_url": "https://www.growthbusiness.co.uk/example",
            "source_type": "Filing",
            "source_name": "Companies House",
            "context_quote": "Victoria Hartley disclosed a £200,000 EIS investment in Brewdog plc via Companies House annual confirmation statement.",
            "linkedin_url": "https://linkedin.com/in/example",
            "date_found": "2026-02-10"
        },
        {
            "name": "Thomas Blackwood",
            "role": "CTO",
            "company": "Nexus Digital",
            "eis_company": "Monzo",
            "sector": "Fintech",
            "amount": "£100,000",
            "source_url": "https://twitter.com/example",
            "source_type": "Social Media",
            "source_name": "X (Twitter)",
            "context_quote": "Excited to announce my personal EIS investment in @monzo. Believe this team will reshape UK banking. #EIS #Fintech",
            "linkedin_url": "https://linkedin.com/in/example",
            "date_found": "2026-02-05"
        },
        {
            "name": "Fiona Gallagher",
            "role": "Investment Director",
            "company": "Meridian Capital",
            "eis_company": "Seedrs",
            "sector": "Fintech",
            "amount": "£50,000",
            "source_url": "https://www.uktech.news/example",
            "source_type": "News",
            "source_name": "UKTN",
            "context_quote": "Fiona Gallagher of Meridian Capital made a personal £50,000 EIS-qualifying investment into equity crowdfunding platform Seedrs.",
            "linkedin_url": "https://linkedin.com/in/example",
            "date_found": "2026-01-28"
        },
        {
            "name": "Robert Ashworth",
            "role": "Founder",
            "company": "Ashworth Ventures",
            "eis_company": "Darktrace",
            "sector": "Cybersecurity",
            "amount": "£300,000",
            "source_url": "https://www.reuters.com/example",
            "source_type": "News",
            "source_name": "Reuters",
            "context_quote": "Robert Ashworth, founder of Ashworth Ventures, has made a significant EIS-qualifying personal investment of £300,000 in cybersecurity firm Darktrace.",
            "linkedin_url": "https://linkedin.com/in/example",
            "date_found": "2026-01-20"
        },
        {
            "name": "Emma Prescott",
            "role": "CFO",
            "company": "Sterling Advisory Group",
            "eis_company": "Crowdcube",
            "sector": "Fintech",
            "amount": "Undisclosed",
            "source_url": "https://www.altfi.com/example",
            "source_type": "News",
            "source_name": "AltFi",
            "context_quote": "Emma Prescott, CFO of Sterling Advisory, has personally backed Crowdcube through the EIS, according to AltFi sources.",
            "linkedin_url": None,
            "date_found": "2026-01-15"
        },
        {
            "name": "Daniel Okonkwo",
            "role": "Portfolio Manager",
            "company": "Atlas Fund Management",
            "eis_company": "Cazoo",
            "sector": "Automotive / E-commerce",
            "amount": "£125,000",
            "source_url": "https://www.thisismoney.co.uk/example",
            "source_type": "News",
            "source_name": "This is Money",
            "context_quote": "Daniel Okonkwo disclosed a personal £125,000 EIS investment in Cazoo, the online car retailer.",
            "linkedin_url": "https://linkedin.com/in/example",
            "date_found": "2026-01-08"
        },
        {
            "name": "Priya Sharma",
            "role": "Managing Partner",
            "company": "Sharma & Co Wealth",
            "eis_company": "Octopus Energy",
            "sector": "Clean Energy",
            "amount": "£250,000",
            "source_url": "https://www.investmentweek.co.uk/example",
            "source_type": "Filing",
            "source_name": "Investment Week",
            "context_quote": "Priya Sharma, Managing Partner at Sharma & Co Wealth, made a £250,000 EIS investment into Octopus Energy Group.",
            "linkedin_url": "https://linkedin.com/in/example",
            "date_found": "2025-12-18"
        },
        {
            "name": "Alex Drummond",
            "role": "Head of Strategy",
            "company": "Pinnacle Holdings",
            "eis_company": "Babylon Health",
            "sector": "HealthTech",
            "amount": "£80,000",
            "source_url": "https://forum.example.com/eis-thread",
            "source_type": "Forum",
            "source_name": "UK Investor Forum",
            "context_quote": "Alex Drummond confirmed on an investor forum that they made an £80,000 EIS-qualifying investment in Babylon Health.",
            "linkedin_url": None,
            "date_found": "2025-12-10"
        },
        {
            "name": "Catherine Townsend",
            "role": "Director",
            "company": "Townsend Financial",
            "eis_company": "Deliveroo",
            "sector": "Food Delivery / Logistics",
            "amount": "£175,000",
            "source_url": "https://www.standard.co.uk/example",
            "source_type": "News",
            "source_name": "Evening Standard",
            "context_quote": "Catherine Townsend, Director of Townsend Financial, made a personal EIS investment of £175,000 in Deliveroo ahead of its IPO.",
            "linkedin_url": "https://linkedin.com/in/example",
            "date_found": "2025-11-25"
        },
        {
            "name": "George Patterson",
            "role": "Private Investor",
            "company": "Independent",
            "eis_company": "Wise (TransferWise)",
            "sector": "Fintech",
            "amount": "Undisclosed",
            "source_url": "https://www.sifted.eu/example",
            "source_type": "News",
            "source_name": "Sifted",
            "context_quote": "Private investor George Patterson is understood to have made an EIS-qualifying investment in TransferWise (now Wise) during its pre-IPO round.",
            "linkedin_url": None,
            "date_found": "2025-11-12"
        },
        {
            "name": "Harriet Morrison",
            "role": "Senior Analyst",
            "company": "Threadneedle Investments",
            "eis_company": "Checkout.com",
            "sector": "Fintech",
            "amount": "£90,000",
            "source_url": "https://www.bloomberg.com/example",
            "source_type": "News",
            "source_name": "Bloomberg",
            "context_quote": "Harriet Morrison of Threadneedle Investments made a personal £90,000 EIS investment in payments firm Checkout.com.",
            "linkedin_url": "https://linkedin.com/in/example",
            "date_found": "2025-10-30"
        },
        {
            "name": "William Crane",
            "role": "Venture Partner",
            "company": "Crane Capital",
            "eis_company": "Improbable",
            "sector": "Gaming / Metaverse",
            "amount": "£500,000",
            "source_url": "https://www.wired.co.uk/example",
            "source_type": "News",
            "source_name": "WIRED UK",
            "context_quote": "William Crane, Venture Partner at Crane Capital, disclosed a £500,000 EIS investment into metaverse startup Improbable.",
            "linkedin_url": "https://linkedin.com/in/example",
            "date_found": "2025-10-15"
        }
    ]

    for inv in sample_data:
        db.execute("""
            INSERT INTO investors (name, role, company, eis_company, sector, amount,
            source_url, source_type, source_name, context_quote, linkedin_url, date_found)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            inv["name"], inv["role"], inv["company"], inv["eis_company"],
            inv["sector"], inv["amount"], inv["source_url"], inv["source_type"],
            inv["source_name"], inv["context_quote"], inv["linkedin_url"], inv["date_found"]
        ))
    db.commit()


# --- App setup ---

db = get_db()
init_db(db)
seed_db(db)


@asynccontextmanager
async def lifespan(app):
    yield
    db.close()


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
    source_type: Optional[str] = Query(None),
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
        conditions.append("(name LIKE ? OR company LIKE ? OR eis_company LIKE ? OR role LIKE ?)")
        s = f"%{search}%"
        params.extend([s, s, s, s])

    if source_type:
        conditions.append("source_type = ?")
        params.append(source_type)

    if sector:
        conditions.append("sector = ?")
        params.append(sector)

    if date_from:
        conditions.append("date_found >= ?")
        params.append(date_from)

    if date_to:
        conditions.append("date_found <= ?")
        params.append(date_to)

    where = " AND ".join(conditions) if conditions else "1=1"

    # Validate sort
    allowed_sort = {"date_found", "name", "eis_company", "sector", "amount", "created_at"}
    if sort_by not in allowed_sort:
        sort_by = "date_found"
    if sort_dir not in ("asc", "desc"):
        sort_dir = "desc"

    # Count
    count_sql = f"SELECT COUNT(*) as total FROM investors WHERE {where}"
    total = db.execute(count_sql, params).fetchone()["total"]

    # Fetch
    offset = (page - 1) * per_page
    data_sql = f"SELECT * FROM investors WHERE {where} ORDER BY {sort_by} {sort_dir} LIMIT ? OFFSET ?"
    rows = db.execute(data_sql, params + [per_page, offset]).fetchall()

    return {
        "investors": rows_to_list(rows),
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": math.ceil(total / per_page) if total > 0 else 1,
    }


@app.get("/api/investors/{investor_id}")
def get_investor(investor_id: int):
    row = db.execute("SELECT * FROM investors WHERE id = ?", [investor_id]).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Investor not found")
    return row_to_dict(row)


@app.get("/api/stats")
def get_stats():
    total = db.execute("SELECT COUNT(*) as c FROM investors").fetchone()["c"]

    # New this week (last 7 days)
    week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    new_this_week = db.execute(
        "SELECT COUNT(*) as c FROM investors WHERE date_found >= ?", [week_ago]
    ).fetchone()["c"]

    # Top sector
    top_sector_row = db.execute(
        "SELECT sector, COUNT(*) as c FROM investors GROUP BY sector ORDER BY c DESC LIMIT 1"
    ).fetchone()
    top_sector = top_sector_row["sector"] if top_sector_row else "N/A"

    # Source types scanned
    sources = db.execute("SELECT COUNT(DISTINCT source_type) as c FROM investors").fetchone()["c"]

    # Unique sectors list
    sectors = [r["sector"] for r in db.execute("SELECT DISTINCT sector FROM investors ORDER BY sector").fetchall()]

    # Source types list
    source_types = [r["source_type"] for r in db.execute("SELECT DISTINCT source_type FROM investors ORDER BY source_type").fetchall()]

    return {
        "total_investors": total,
        "new_this_week": new_this_week,
        "top_sector": top_sector,
        "sources_scanned": sources,
        "sectors": sectors,
        "source_types": source_types,
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

    for inv in batch.investors:
        # Check for duplicate by name + eis_company
        existing = db.execute(
            "SELECT id FROM investors WHERE name = ? AND eis_company = ?",
            [inv.name, inv.eis_company]
        ).fetchone()

        if existing:
            # Update existing record
            db.execute("""
                UPDATE investors SET role=?, company=?, sector=?, amount=?,
                source_url=?, source_type=?, source_name=?, context_quote=?,
                linkedin_url=?, date_found=?
                WHERE id=?
            """, (
                inv.role, inv.company, inv.sector, inv.amount,
                inv.source_url, inv.source_type, inv.source_name, inv.context_quote,
                inv.linkedin_url, inv.date_found, existing["id"]
            ))
            skipped += 1
        else:
            db.execute("""
                INSERT INTO investors (name, role, company, eis_company, sector, amount,
                source_url, source_type, source_name, context_quote, linkedin_url, date_found)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                inv.name, inv.role, inv.company, inv.eis_company, inv.sector, inv.amount,
                inv.source_url, inv.source_type, inv.source_name, inv.context_quote,
                inv.linkedin_url, inv.date_found
            ))
            inserted += 1

    db.commit()
    return {"inserted": inserted, "updated": skipped, "total": len(batch.investors)}


@app.get("/api/export/last")
def get_last_export():
    """Return info about the last 'new' export."""
    row = db.execute(
        "SELECT exported_at, investor_count FROM export_log WHERE export_type = 'new' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if row:
        # Count investors added since that export
        new_since = db.execute(
            "SELECT COUNT(*) as c FROM investors WHERE created_at > ?", [row["exported_at"]]
        ).fetchone()["c"]
        return {
            "last_exported_at": row["exported_at"],
            "last_export_count": row["investor_count"],
            "new_since_last_export": new_since,
        }
    else:
        total = db.execute("SELECT COUNT(*) as c FROM investors").fetchone()["c"]
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
        conditions.append("(name LIKE ? OR company LIKE ? OR eis_company LIKE ? OR role LIKE ?)")
        s = f"%{search}%"
        params.extend([s, s, s, s])
    if source_type:
        conditions.append("source_type = ?")
        params.append(source_type)
    if sector:
        conditions.append("sector = ?")
        params.append(sector)
    if date_from:
        conditions.append("date_found >= ?")
        params.append(date_from)
    if date_to:
        conditions.append("date_found <= ?")
        params.append(date_to)

    where = " AND ".join(conditions) if conditions else "1=1"
    allowed_sort = {"date_found", "name", "eis_company", "sector", "amount", "created_at"}
    if sort_by not in allowed_sort:
        sort_by = "date_found"
    if sort_dir not in ("asc", "desc"):
        sort_dir = "desc"

    rows = db.execute(
        f"SELECT * FROM investors WHERE {where} ORDER BY {sort_by} {sort_dir}", params
    ).fetchall()
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
    # Find last export timestamp
    last_row = db.execute(
        "SELECT exported_at FROM export_log WHERE export_type = 'new' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    last_exported_at = last_row["exported_at"] if last_row else None

    if last_exported_at:
        rows = db.execute(
            "SELECT * FROM investors WHERE created_at > ? ORDER BY created_at DESC",
            [last_exported_at],
        ).fetchall()
    else:
        rows = db.execute("SELECT * FROM investors ORDER BY created_at DESC").fetchall()

    investors = [dict(r) for r in rows]

    if len(investors) == 0:
        raise HTTPException(status_code=404, detail="No new investors since last export.")

    now = datetime.now()
    now_str = now.strftime('%d %B %Y at %H:%M')
    since_str = last_exported_at[:16].replace("T", " ") if last_exported_at else "the beginning"
    buffer = build_excel(
        investors,
        title_text="EIS Investor Collector \u2014 New Since Last Export",
        subtitle_text=f"Generated {now_str}  \u00b7  {len(investors)} new investors since {since_str}",
    )

    # Record this export
    db.execute(
        "INSERT INTO export_log (exported_at, investor_count, export_type) VALUES (?, ?, 'new')",
        [now.strftime("%Y-%m-%dT%H:%M:%S"), len(investors)],
    )
    db.commit()

    filename = f"eis_investors_new_{now.strftime('%Y-%m-%d')}.xlsx"
    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/email/new-investors")
def get_new_investors_for_email():
    """Return investors added since last email, and record the email event."""
    last_row = db.execute(
        "SELECT emailed_at FROM email_log ORDER BY id DESC LIMIT 1"
    ).fetchone()
    last_emailed_at = last_row["emailed_at"] if last_row else None

    if last_emailed_at:
        rows = db.execute(
            "SELECT name, role, company, eis_company, sector, amount, source_name, date_found "
            "FROM investors WHERE created_at > ? ORDER BY created_at DESC",
            [last_emailed_at],
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT name, role, company, eis_company, sector, amount, source_name, date_found "
            "FROM investors ORDER BY created_at DESC"
        ).fetchall()

    investors = [dict(r) for r in rows]
    return {
        "investors": investors,
        "count": len(investors),
        "since": last_emailed_at,
    }


@app.post("/api/email/mark-sent")
def mark_email_sent(count: int = Query(0)):
    """Record that an email digest was sent."""
    now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    db.execute(
        "INSERT INTO email_log (emailed_at, investor_count) VALUES (?, ?)",
        [now, count],
    )
    db.commit()
    return {"status": "recorded", "emailed_at": now, "count": count}


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
