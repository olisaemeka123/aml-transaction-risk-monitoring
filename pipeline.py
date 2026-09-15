"""dlt pipeline: pulls transactions and their (separately-served) ground
truth labels from the FastAPI feed into BigQuery raw tables.

Two resources, two different incremental strategies, deliberately:

- transactions: cursor-based on `timestamp`. dlt remembers the maximum
  timestamp seen across ALL previous runs (dlt.sources.incremental), and
  within a single run we page through results with since/limit until a page
  comes back short.

- transaction_labels: cursor-based on `sequence`, an integer stamped onto
  each label at generation time (see main.py) rather than a timestamp,
  since labels have no timestamp of their own. Same dlt mechanism, different
  field -- proof that dlt's incremental tracking isn't tied to timestamps
  specifically, just to any value that only increases over time.

Requires the API (main.py) to already be running on API_BASE before this
executes -- it does not start the API itself.

Run with: python3 pipeline.py
"""
import dlt
import requests

API_BASE = "http://127.0.0.1:8000"
PAGE_SIZE = 5000


@dlt.resource(name="transactions", primary_key="transaction_id", write_disposition="append")
def transactions_resource(
    updated_at=dlt.sources.incremental("timestamp", initial_value="1970-01-01T00:00:00+00:00")
):
    """Pages through /transactions starting from wherever the last run left
    off (updated_at.last_value), advancing the cursor with each page's final
    timestamp, until a page comes back smaller than PAGE_SIZE -- meaning
    we've caught up to the live edge of the feed for this run."""
    since = updated_at.last_value
    while True:
        response = requests.get(
            f"{API_BASE}/transactions",
            params={"since": since, "limit": PAGE_SIZE},
        )
        response.raise_for_status()
        page = response.json()
        if not page:
            break
        yield page
        since = page[-1]["timestamp"]
        if len(page) < PAGE_SIZE:
            break


@dlt.resource(name="transaction_labels", primary_key="transaction_id", write_disposition="append")
def labels_resource(
    seq=dlt.sources.incremental("sequence", initial_value=-1)
):
    """Pages through /transactions/labels by position, resuming from one
    past the last sequence number seen in any previous run. Structurally
    identical to transactions_resource above, just cursoring on `sequence`
    instead of `timestamp`."""
    offset = seq.last_value + 1
    while True:
        response = requests.get(
            f"{API_BASE}/transactions/labels",
            params={"offset": offset, "limit": PAGE_SIZE},
        )
        response.raise_for_status()
        page = response.json()
        if not page:
            break
        yield page
        offset += len(page)
        if len(page) < PAGE_SIZE:
            break


def run():
    pipeline = dlt.pipeline(
        pipeline_name="transaction_risk_pipeline",
        destination="bigquery",
        dataset_name="raw_transaction_risk",
    )
    load_info = pipeline.run([transactions_resource(), labels_resource()])
    print(load_info)


if __name__ == "__main__":
    run()
