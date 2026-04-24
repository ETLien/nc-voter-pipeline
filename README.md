# NC Voter Data Pipeline

This project is for anyone who wants to build and maintain their own local database of North Carolina voter data — independently, on their own hardware, using public data from the [NC State Board of Elections (NCSBE)](https://www.ncsbe.gov/results-data/voter-registration-data).

## What This Does

NCSBE publishes weekly statewide snapshots of voter registration and voting history. Rather than just keeping the latest snapshot, this pipeline tracks changes over time — capturing who changed their party, moved, voted in a new election, or registered for the first time. The result is a historical PostgreSQL database you own and control, suitable for your own research, analysis, or applications.

**Two data streams:**

- **Voter registration** (`nc_vreg_history`) — tracks how individual voter records change over time. When a voter updates their party affiliation, address, precinct, or any other field, the new record is appended with a `data_date` reflecting when NCSBE published it. The full history of changes per voter is preserved.
- **Voting history** (`nc_vhis_history`) — a cumulative log of every election each voter has participated in. New participation records are appended each week; existing records are never duplicated.

---

## Prerequisites

- Python 3.x
- PostgreSQL (local instance)
- The two target tables (`nc_vreg_history`, `nc_vhis_history`) must already exist in your database

**Install dependencies:**
```
pip install pandas "psycopg[binary]" sqlalchemy schedule
```

Or using the project venv:
```
.venv\Scripts\pip install pandas "psycopg[binary]" sqlalchemy schedule
```

---

## Files

### `nc_etl.py` — Main Pipeline

The primary script. Run it once and it stays running, checking for new data every hour.

```
python nc_etl.py
```

**How it works:**

1. Sends a HEAD request to S3 for each file — no download, just checks the `Last-Modified` timestamp
2. Compares against `last_modified.json` (the local cache)
3. If a file's timestamp has changed or its previous run did not complete, downloads and processes it
4. Archives a TSV backup of both SQL tables to `D:\nc_archive` before any changes are made
5. Downloads the updated zip, saves a dated copy to `D:\nc_archive`, unzips it
6. Processes the file using chunked reads (100k rows at a time) and SQL-side deduplication — the master table never loads into Python memory
7. Updates `last_modified.json` per file only after successful processing

**S3 outage handling:** If NCSBE's S3 is unreachable or returns an error, the check is skipped and the scheduler tries again next hour. The script never crashes from a network failure.

**Crash recovery:** `last_modified.json` tracks a `processed` flag per file. If the script crashes mid-run, the flag stays `false` and the next hourly check automatically retries that file. Because both processing functions are idempotent (SQL dedup skips already-inserted records), re-processing is always safe.

---

### `recreate_masters.py` — Disaster Recovery

Rebuilds one or both SQL tables from scratch using the archived zip files in `D:\nc_archive`. Use this if the tables are lost or corrupted.

```
python recreate_masters.py           # rebuild both tables
python recreate_masters.py --vreg    # nc_vreg_history only
python recreate_masters.py --vhis    # nc_vhis_history only
```

Requires typing `YES` at the confirmation prompt before truncating anything.

**How it differs from the weekly pipeline:**
- Drops indexes before bulk loading and recreates them at the end (much faster for large backlogs)
- Uses UNLOGGED staging tables (skips write-ahead logging on throwaway data)
- Loads each file's chunks into a single staging table and runs one anti-join per file instead of per chunk
- Uses a larger chunk size (500k rows) since this is a one-time operation

---

### `last_modified.json` — S3 Cache

Tracks the S3 `Last-Modified` timestamp and processing status for each file. Created automatically after the first successful run.

**Seed this file before starting `nc_etl.py` for the first time** if you have already loaded data (e.g., via a previous version of the pipeline), so the script doesn't re-process data already in your tables.

To get the current S3 timestamps:
```
.venv\Scripts\python -c "import urllib.request; req = urllib.request.Request('https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvoter_Statewide.zip', method='HEAD'); r = urllib.request.urlopen(req); print(r.headers.get('Last-Modified'))"
```

Format:
```json
{
  "ncvoter_Statewide.zip": {"timestamp": "Mon, 20 Apr 2026 00:10:13 GMT", "processed": true},
  "ncvhis_Statewide.zip":  {"timestamp": "Mon, 20 Apr 2026 00:09:53 GMT", "processed": true}
}
```

---

## Data Sources

| File | URL |
|------|-----|
| Voter Registration | `https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvoter_Statewide.zip` |
| Voting History | `https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvhis_Statewide.zip` |

NCSBE publishes updated files weekly, typically on Sundays. The pipeline detects updates via the S3 `Last-Modified` header and uses that date as the `data_date` in the database — not the time the script ran.

---

## Archive Structure

`D:\nc_archive` contains:
- `YYYYMMDD_ncvoter_Statewide.zip` — dated copy of each downloaded registration file
- `YYYYMMDD_ncvhis_Statewide.zip` — dated copy of each downloaded history file
- `nc_vreg_history.tsv` — TSV backup of the registration table (overwritten each run)
- `nc_vhis_history.tsv` — TSV backup of the history table (overwritten each run)

The dated zip files are what `recreate_masters.py` reads from to rebuild the tables.

---

## Database

| Setting | Value |
|---------|-------|
| Host | `localhost` |
| Port | `5432` |
| Database | `state_voter_data` |
| Schema | `public` |
| Registration table | `nc_vreg_history` |
| History table | `nc_vhis_history` |

**Indexes** are created automatically on first run:
- `nc_vreg_history (ncid, data_date DESC)`
- `nc_vhis_history (ncid, election_lbl)`
