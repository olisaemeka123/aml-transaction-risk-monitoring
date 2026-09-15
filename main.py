"""FastAPI app simulating a transaction feed with real production shape: a
large pre-existing historical backlog (seeded once at startup) plus an
ongoing live feed layered on top. dlt discovers ALL of it -- backlog and new
activity alike -- through the same paginated, cursor-based /transactions
endpoint. There is no separate bulk-load path: this mirrors how a first sync
against any real payments API works, paging through years of existing
history before catching up to "now", then continuing incrementally.

Startup takes roughly 30-60 seconds while the 2.5M-transaction backlog
generates in memory -- expected, not a bug. Memory footprint is around
1-1.5GB while running. A known, documented simplification for a local
portfolio demo: resets on restart, does not scale or persist.

Ground truth labels are served from a SEPARATE endpoint (/transactions/labels).
This mirrors how real fraud labels typically arrive later, from a different
system (investigations, chargebacks), than the transaction feed itself --
detection logic in dbt should never touch this endpoint's data, only the
validation model should.

Run with: uvicorn main:app --reload
Docs at:  http://127.0.0.1:8000/docs
"""
import bisect
import random
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, Query

from generator import generate_batch, generate_historical_backlog

TOTAL_HISTORICAL_TRANSACTIONS = 2_500_000
TOTAL_HISTORICAL_FRAUD = 10_000
HISTORY_DAYS = 180

# In-memory store, kept sorted by timestamp ascending throughout the app's
# life: the historical backlog is sorted once at startup, and every live
# transaction appended afterward has a timestamp strictly later than
# everything before it, so the list never needs re-sorting.
ALL_TRANSACTIONS = []
ALL_LABELS = []
TIMESTAMPS = []  # parallel to ALL_TRANSACTIONS, enables fast cursor lookups via bisect
_last_ts = None


def _assign_sequence(new_labels):
    """Stamps each new label with its position in ALL_LABELS before it gets
    appended -- gives dlt a monotonically increasing field to track for
    incremental extraction, the same role `timestamp` plays for transactions,
    since labels have no timestamp of their own."""
    start = len(ALL_LABELS)
    for i, label in enumerate(new_labels):
        label["sequence"] = start + i


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _last_ts
    print(f"Seeding {TOTAL_HISTORICAL_TRANSACTIONS:,} historical transactions "
          f"({TOTAL_HISTORICAL_FRAUD:,} fraud) -- this takes under a minute...")
    txns, labels = generate_historical_backlog(
        total_transactions=TOTAL_HISTORICAL_TRANSACTIONS,
        total_fraud=TOTAL_HISTORICAL_FRAUD,
        history_days=HISTORY_DAYS,
    )
    ALL_TRANSACTIONS.extend(txns)
    _assign_sequence(labels)
    ALL_LABELS.extend(labels)
    TIMESTAMPS.extend(t["timestamp"] for t in txns)
    _last_ts = datetime.fromisoformat(ALL_TRANSACTIONS[-1]["timestamp"])
    print(f"Ready. {len(ALL_TRANSACTIONS):,} transactions in memory "
          f"({100 * TOTAL_HISTORICAL_FRAUD / TOTAL_HISTORICAL_TRANSACTIONS:.3f}% fraud).")
    yield


app = FastAPI(title="Transaction Risk Monitoring - Simulated Feed", lifespan=lifespan)


def _advance_live():
    """Appends a small new batch simulating time having passed since the
    last poll -- the exact same generation logic as the historical backlog,
    just continuing forward from where it left off."""
    global _last_ts
    n = random.randint(10, 30)
    new_txns, new_labels = generate_batch(n=n, start_ts=_last_ts)
    if new_txns:
        _last_ts = datetime.fromisoformat(new_txns[-1]["timestamp"])
    ALL_TRANSACTIONS.extend(new_txns)
    _assign_sequence(new_labels)
    ALL_LABELS.extend(new_labels)
    TIMESTAMPS.extend(t["timestamp"] for t in new_txns)


@app.get("/transactions")
def get_transactions(
    since: Optional[str] = Query(default=None, description="ISO timestamp cursor; returns transactions after this point"),
    limit: int = Query(default=5000, le=20000, description="Max rows returned per page"),
):
    """Paginated, cursor-based endpoint -- the same shape as a real payments
    API. dlt should call this repeatedly, using the timestamp of the last
    row returned as the next `since`, until a page returns fewer than
    `limit` rows (meaning the backlog is exhausted). Continuing to poll from
    there surfaces genuinely new live transactions as they're generated."""
    _advance_live()
    start_idx = 0 if since is None else bisect.bisect_right(TIMESTAMPS, since)
    return ALL_TRANSACTIONS[start_idx:start_idx + limit]


@app.get("/transactions/labels")
def get_labels(
    offset: int = Query(default=0, ge=0, description="Number of labels to skip"),
    limit: int = Query(default=5000, le=20000, description="Max rows returned per page"),
):
    """Ground truth labels for the validation model only. Never join this
    into detection logic -- a real system wouldn't have it at flag time.

    Paginated by list position rather than a timestamp cursor: labels have no
    timestamp of their own, but are always appended in lockstep with
    transactions, so position-based paging is correct here, not a shortcut."""
    return ALL_LABELS[offset:offset + limit]


@app.get("/health")
def health():
    return {"status": "ok", "total_transactions_in_memory": len(ALL_TRANSACTIONS)}
