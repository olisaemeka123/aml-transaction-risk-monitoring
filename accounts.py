"""Generates and persists a static pool of accounts with country assignments.

Run once (`python accounts.py`) to create accounts.json. The transaction
generator loads this pool rather than regenerating it, so account_ids and
their countries stay stable across generator/API restarts.
"""
import json
import random
import uuid

# Countries considered high-risk for the jurisdiction detection rule.
# Deliberately fictional codes rather than real country names -- this avoids
# implying any real nation is AML-risk in a public GitHub repo. This list
# MUST match the dbt seed (seeds/high_risk_countries.csv) exactly, or the
# jurisdiction rule will silently never fire.
HIGH_RISK_COUNTRIES = ["ZX", "ZY", "ZZ"]
NORMAL_COUNTRIES = ["GB", "US", "DE", "FR", "IE", "NL", "CA", "AU"]

NUM_ACCOUNTS = 100_000
HIGH_RISK_FRACTION = 0.10  # ~10% of accounts domiciled in a high-risk country


def generate_accounts(seed: int = 42) -> list:
    rng = random.Random(seed)
    accounts = []
    for _ in range(NUM_ACCOUNTS):
        is_high_risk = rng.random() < HIGH_RISK_FRACTION
        country = rng.choice(HIGH_RISK_COUNTRIES) if is_high_risk else rng.choice(NORMAL_COUNTRIES)
        accounts.append({
            "account_id": str(uuid.uuid4()),
            "country": country,
        })
    return accounts


if __name__ == "__main__":
    accounts = generate_accounts()
    with open("accounts.json", "w") as f:
        json.dump(accounts, f, indent=2)
    high_risk_count = sum(1 for a in accounts if a["country"] in HIGH_RISK_COUNTRIES)
    print(f"Generated {len(accounts)} accounts -> accounts.json")
    print(f"High-risk country accounts: {high_risk_count}")
