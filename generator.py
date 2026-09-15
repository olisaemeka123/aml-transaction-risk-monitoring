"""Core transaction generation engine: produces normal transactions and
periodically injects one of five fraud patterns, each tagged with ground
truth for later evaluation.

Ground truth is returned as a SEPARATE record from the transaction itself --
never mixed into the fields a detection model would see. This mirrors real
fraud detection, where confirmed labels typically arrive later from a
different system (investigations, chargebacks) than the transaction feed.
"""
import random
import uuid
import json
from datetime import datetime, timedelta, timezone

from accounts import HIGH_RISK_COUNTRIES

with open("accounts.json") as f:
    ACCOUNTS = json.load(f)

ACCOUNT_IDS = [a["account_id"] for a in ACCOUNTS]
HIGH_RISK_ACCOUNT_IDS = [a["account_id"] for a in ACCOUNTS if a["country"] in HIGH_RISK_COUNTRIES]

# Controls the rate of fraud EVENTS, not fraud TRANSACTIONS -- because velocity
# and structuring patterns are bursts (6-12 and 3-5 transactions per event)
# while normal transactions and the other three fraud patterns are single
# transactions, the resulting transaction-level fraud rate ends up several
# times higher than this number. Calibrated by direct testing to produce a
# ~0.4% transaction-level fraud rate, matching the backfill's exact 10,000-in-
# 2,500,000 ratio -- so the live feed continues at the same realistic fraud
# proportion the historical dataset was built with, rather than drifting to a
# different rate over time.
FRAUD_INJECTION_RATE = 0.0013


def _make_pair(sender, receiver, amount, ts, is_fraud, pattern=None):
    """Builds the (transaction, label) pair sharing one transaction_id."""
    txn_id = str(uuid.uuid4())
    txn = {
        "transaction_id": txn_id,
        "timestamp": ts.isoformat(),
        "sender_account_id": sender,
        "receiver_account_id": receiver,
        "amount": round(amount, 2),
    }
    label = {
        "transaction_id": txn_id,
        "is_fraud_ground_truth": is_fraud,
        "fraud_pattern": pattern,  # kept for our own dev/debugging only, not a real-world field
    }
    return txn, label


def generate_normal_transaction(ts):
    sender, receiver = random.sample(ACCOUNT_IDS, 2)
    # Lognormal: most transactions small, a realistic long tail of larger ones.
    amount = random.lognormvariate(mu=4.0, sigma=1.0)
    return [_make_pair(sender, receiver, amount, ts, is_fraud=False)]


def generate_velocity_fraud(ts):
    """Burst of several transactions from one account within a short window."""
    account = random.choice(ACCOUNT_IDS)
    others = [a for a in ACCOUNT_IDS if a != account]
    burst = []
    for i in range(random.randint(6, 12)):
        receiver = random.choice(others)
        amount = random.uniform(50, 500)
        txn_ts = ts + timedelta(seconds=random.randint(1, 120) * i)
        burst.append(_make_pair(account, receiver, amount, txn_ts, is_fraud=True, pattern="velocity"))
    return burst


def generate_outlier_fraud(ts):
    """One transaction far larger than the sender's typical amount."""
    sender, receiver = random.sample(ACCOUNT_IDS, 2)
    amount = random.uniform(5000, 20000)  # well above the normal lognormal range
    return [_make_pair(sender, receiver, amount, ts, is_fraud=True, pattern="outlier")]


def generate_structuring_fraud(ts):
    """Several transactions clustering just under the £10,000 reporting threshold."""
    account = random.choice(ACCOUNT_IDS)
    others = [a for a in ACCOUNT_IDS if a != account]
    batch = []
    for i in range(random.randint(3, 5)):
        receiver = random.choice(others)
        amount = random.uniform(9000, 9950)
        txn_ts = ts + timedelta(minutes=random.randint(5, 90) * i)
        batch.append(_make_pair(account, receiver, amount, txn_ts, is_fraud=True, pattern="structuring"))
    return batch


def generate_new_payee_fraud(ts):
    """A high-value transaction to a receiver the sender hasn't used before.
    True 'first time' status is verified downstream in dbt from real history --
    this just aims the generator at producing a plausible instance of the pattern."""
    sender, receiver = random.sample(ACCOUNT_IDS, 2)
    amount = random.uniform(3000, 15000)
    return [_make_pair(sender, receiver, amount, ts, is_fraud=True, pattern="new_payee_high_value")]


def generate_jurisdiction_fraud(ts):
    """A transaction involving an account domiciled in a high-risk country."""
    if not HIGH_RISK_ACCOUNT_IDS:
        return generate_outlier_fraud(ts)  # fallback, shouldn't happen with default pool
    high_risk_account = random.choice(HIGH_RISK_ACCOUNT_IDS)
    other = random.choice([a for a in ACCOUNT_IDS if a != high_risk_account])
    sender, receiver = (high_risk_account, other) if random.random() < 0.5 else (other, high_risk_account)
    amount = random.uniform(500, 8000)
    return [_make_pair(sender, receiver, amount, ts, is_fraud=True, pattern="jurisdiction")]


FRAUD_GENERATORS = [
    generate_velocity_fraud,
    generate_outlier_fraud,
    generate_structuring_fraud,
    generate_new_payee_fraud,
    generate_jurisdiction_fraud,
]


def generate_historical_backlog(total_transactions, total_fraud, history_days=180):
    """Generates a full historical dataset in memory: exactly total_fraud
    fraud transactions split evenly across the 5 patterns, filling the
    remainder with normal transactions, spread across the last history_days
    days, sorted chronologically.

    Used to seed the API's in-memory state at startup so dlt's first sync
    encounters a realistic pre-existing backlog -- the same way a first sync
    against any real payments API would find years of existing history to
    page through before catching up to "now". There is no separate bulk-load
    path; dlt discovers this backlog through the same paginated endpoint it
    uses for ongoing incremental polling."""
    fraud_per_pattern = total_fraud // 5
    window_end = datetime.now(timezone.utc)
    window_start = window_end - timedelta(days=history_days)
    window_seconds = (window_end - window_start).total_seconds()

    def random_ts():
        return window_start + timedelta(seconds=random.uniform(0, window_seconds))

    def generate_exact_count(pattern_fn, target_count):
        produced = []
        while len(produced) < target_count:
            pairs = pattern_fn(random_ts())
            for txn, label in pairs:
                if len(produced) >= target_count:
                    break
                produced.append((txn, label))
        return produced

    all_pairs = []
    for pattern_fn in FRAUD_GENERATORS:
        all_pairs.extend(generate_exact_count(pattern_fn, fraud_per_pattern))

    normal_target = total_transactions - len(all_pairs)
    for _ in range(normal_target):
        txn, label = generate_normal_transaction(random_ts())[0]
        all_pairs.append((txn, label))

    all_pairs.sort(key=lambda p: p[0]["timestamp"])
    transactions = [p[0] for p in all_pairs]
    labels = [p[1] for p in all_pairs]
    return transactions, labels


def generate_batch(n, start_ts):
    """Generates n 'events' starting from start_ts -- a normal single
    transaction, or a fraud pattern (which may itself contain several
    transactions). Returns (transactions, labels) as parallel lists, and the
    latest timestamp reached, so the caller can advance its clock."""
    transactions = []
    labels = []
    ts = start_ts
    for _ in range(n):
        ts = ts + timedelta(seconds=random.randint(5, 60))
        if random.random() < FRAUD_INJECTION_RATE:
            pattern_fn = random.choice(FRAUD_GENERATORS)
            pairs = pattern_fn(ts)
        else:
            pairs = generate_normal_transaction(ts)
        for txn, label in pairs:
            transactions.append(txn)
            labels.append(label)
        if pairs:
            ts = max(ts, datetime.fromisoformat(pairs[-1][0]["timestamp"]))
    return transactions, labels
