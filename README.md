# NC Voter Data Pipeline

This project is for anyone who wants to build and maintain their own local
database of North Carolina voter data — independently, on their own hardware,
using public data from the [NC State Board of Elections (NCSBE)](https://www.ncsbe.gov/results-data/voter-registration-data).

## What This Does

NCSBE publishes weekly statewide snapshots of voter registration and voting
history. Rather than just keeping the latest snapshot, this pipeline tracks
changes over time — capturing who changed their party, moved, voted in a new
election, or registered for the first time. The result is a historical
PostgreSQL database you own and control, suitable for your own research,
analysis, or applications.

**Two data streams:**

- **Voter registration** (`nc_vreg_history`) — tracks how individual voter
records change over time. When a voter updates their party affiliation, address,
precinct, or any other field, the new record is appended with a `data_date`
reflecting when NCSBE published it. The full history of changes per voter is
preserved.
- **Voting history** (`nc_vhis_history`) — a cumulative log of every election
each voter has participated in. New participation records are appended each
week; existing records are never duplicated.

---

## Prerequisites

- Python 3.x
- PostgreSQL (local instance)
- The two target tables (`nc_vreg_history`, `nc_vhis_history`) must already
exist in your database

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

### `nc_etl_v7.py` — Main Pipeline

The primary script. Run it once and it stays running, checking for new data
every hour.

```
python nc_etl.py
```

**How it works:**

1. Sends a HEAD request to S3 for each file — no download, just checks the
`Last-Modified` timestamp
2. Compares against `last_modified.json` (the local cache)
3. If a file's timestamp has changed or its previous run did not complete,
downloads and processes it
4. Archives a TSV backup of both SQL tables to `D:\nc_archive` before any
changes are made
5. Downloads the updated zip, saves a dated copy to `D:\nc_archive`, unzips it
6. Processes the file using chunked reads (100k rows at a time) and SQL-side
deduplication — the master table never loads into Python memory
7. Updates `last_modified.json` per file only after successful processing

**S3 outage handling:** If NCSBE's S3 is unreachable or returns an error, the
check is skipped and the scheduler tries again next hour. The script never
crashes from a network failure.

**Crash recovery:** `last_modified.json` tracks a `processed` flag per file. If
the script crashes mid-run, the flag stays `false` and the next hourly check
automatically retries that file. Because both processing functions are
idempotent (SQL dedup skips already-inserted records), re-processing is always
safe.

---

### `recreate_masters.py` — Disaster Recovery

Rebuilds one or both SQL tables from scratch using the archived zip files in
`D:\nc_archive`. Use this if the tables are lost or corrupted.

```
python recreate_masters.py           # rebuild both tables
python recreate_masters.py --vreg    # nc_vreg_history only
python recreate_masters.py --vhis    # nc_vhis_history only
```

Requires typing `YES` at the confirmation prompt before truncating anything.

**How it differs from the weekly pipeline:**
- Drops indexes before bulk loading and recreates them at the end (much faster
for large backlogs)
- Uses UNLOGGED staging tables (skips write-ahead logging on throwaway data)
- Loads each file's chunks into a single staging table and runs one anti-join
per file instead of per chunk
- Uses a larger chunk size (500k rows) since this is a one-time operation

---

### `last_modified.json` — S3 Cache

Tracks the S3 `Last-Modified` timestamp and processing status for each file.
Created automatically after the first successful run.

**Seed this file before starting `nc_etl_v7.py` for the first time** if you have
already loaded data (e.g., via a previous version of the pipeline), so the
script doesn't re-process data already in your tables.

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

NCSBE publishes updated files weekly, typically on Sundays. The pipeline detects
updates via the S3 `Last-Modified` header and uses that date as the `data_date`
in the database — not the time the script ran.

---

## Archive Structure

`D:\nc_archive` contains:
- `YYYYMMDD_ncvoter_Statewide.zip` — dated copy of each downloaded registration
file
- `YYYYMMDD_ncvhis_Statewide.zip` — dated copy of each downloaded history file
- `nc_vreg_history.tsv` — TSV backup of the registration table (overwritten each
run)
- `nc_vhis_history.tsv` — TSV backup of the history table (overwritten each run)

The dated zip files are what `recreate_masters.py` reads from to rebuild the
tables.

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

# Known Data Limitations — NC Voter Data Pipeline

This portion tracks known quirks, inconsistencies, and limitations in the source
data from the NC State Board of Elections (NCSBE), as reflected in
`nc_vreg_history` and `nc_vhis_history`. As far as I am aware, these are **not
bugs in this pipeline** (though there certainly may be). They are properties of
the state's source data that this project intentionally preserves rather than
"corrects," in order to maintain an accurate historical record. Analysts should
read this before drawing conclusions from either table.

Findings below are dated to indicate when they were last verified. As these
tables continue to grow via the weekly ETL pipeline, the specific
counts/percentages cited will shift. The underlying patterns are not expected to
disappear, but should be reviewed periodically.

## nc_vreg_history

### 1. `registr_dt` contains literal placeholder values
*Last verified: 2026-07-05*

Some rows have `registr_dt = '##/##/####'` instead of a real date. This is
confirmed present in NCSBE's raw source files (verified against files archived
both before and after this project's switch to archiving raw, unmodified
downloads (which I believe should rule out this pipeline as the source)). As of
the latest analysis, this affected ~20K of 6.5M rows (~0.3%) in the table. 

- Not randomly distributed: ~47% of affected rows come from a single
  county (county_id 26, Cumberland County), across all voter statuses
  (active, removed, inactive, denied). This could indicate this is not
  status-specific.
- Counts spike around year-start and post-general-election snapshot dates
  (single-day spikes of several thousand rows have been observed around
  January 1st snapshots).
- Unfortunately, toot cause unconfirmed. It could originate with the state,
  a specific county board, or a systems change on either side. I have not
  contacted the state nor do I intend to.
- The column will be retained as `TEXT`, not cast to `DATE`, specifically
  because of the existence of this literal string in the source data.
- **Impact for analysis:** any query assuming every row has a valid
  registration date will need to explicitly filter/handle this pattern.
  The solution may be to locate a valid registr_dt value in a subsequent
  record and then backfill it. I have not attempted to check if this is
  possible (perhaps these NCIDs will have that value in registr_dt forever).
  If it were possible, it also does not guarantee that it would work in every
  case.

### 2. "New" records that reflect code/label changes, not real changes
*Last verified: 2026-07-05*

Because this table is a longitudinal record (one row per `ncid` per
`data_date` snapshot where NCSBE-reported values differ from the prior
snapshot), a change in how the state encodes a field (without any real
change to the voter's actual situation) can still cause a new
`data_date` row to be written.

**Confirmed example:** in at least one large batch, on the 2026-01-04
snapshot, several municipality/VTD abbreviation codes were changed to a
new standard format while the corresponding description stayed
identical (an abbreviation like `SY9` became `SYLV`, but the
description remained `SYLVA` throughout, both before and after the
change, across multiple prior snapshots going back roughly two years).
The same recoding was observed across more than one field
(`munic_dist_abbrv` and `vtd_abbrv`) for affected voters simultaneously,
and affected multiple distinct municipalities/codes in the same batch
(e.g., legacy codes like `WE11`, `D15`, `CULL` were replaced with `WEBS`,
`DILL`, `FHIL` respectively, each with an unchanged description) —
consistent with a systemic, state-side code standardization event rather
than an isolated data entry change.

- This is a confirmed, verified pattern (not hypothetical). Description
  fields remained constant across the recoding, ruling out a real change
  in the voter's municipality/district.
- Cause is presumed to be a state-side systems/coding update; this
  is not confirmed.
- Not currently detected or filtered out by the ETL pipeline.
- **Impact for analysis:** counts of "how many times has this voter's
  record changed" may be inflated by code-format-only snapshots. This
  pipeline does not attempt to distinguish substantive changes from
  code/label-only changes.
- **Note:** other fields in this table follow expected, non-anomalous
  patterns of change (e.g., `age_at_year_end` increments by 1 with each
  annual snapshot, as expected). This limitation applies specifically to
  administrative code/label fields, not the table as a whole.

## nc_vhis_history

### 1. Duplicate rows per `(ncid, election_lbl)` due to changing category labels
*Last verified: 2026-07-05*

This table has no enforced uniqueness constraint. A significant portion
of rows sharing the same `(ncid, election_lbl)` are not true duplicates
in the sense of redundant data entry — at least some reflect NCSBE having
changed its categorical labeling of a field (most notably
`voting_method`) at some point not reflected in its most recently
published file layout documentation.

Example: `"IN-PERSON"` (older records) vs. `"ELECTION DAY IN-PERSON"` /
`"EARLY VOTING IN-PERSON"` (newer records) for what may or may not be the
same underlying category. I have not confirmed as a clean 1:1 mapping.

- NCSBE's most recently published file layout (`layout_ncvhis.txt`, dated
  2021-06-17) documents `voting_method` as `varchar(10)`, which is now
  inaccurate (live data contains values over 20 characters long).
  Whether this labeling change happened at, before, or after that
  document's date is unconfirmed; the date only establishes that the
  published layout is currently stale, not when the drift began. No
  updated layout or mapping table has been published.
- This pipeline does **not** attempt to normalize, merge, or reconcile
  these labels. Doing so would require assuming an equivalence NCSBE has
  not confirmed (whether `"IN-PERSON"` pre-change is equivalent to
  the sum of the newer, more granular categories, or only to one of
  them), which risks asserting a false historical claim rather than
  preserving an accurate one.
- The full scope of duplication in this table (~16.78M duplicate groups
  out of ~16.78M total rows, averaging ~2.04 rows/group, as of initial
  analysis) has not been fully attributed. Voting-method label drift is a
  confirmed contributing factor but has not been confirmed as the sole
  or dominant cause of duplication across the entire table.
- **Impact for analysis:** counting or grouping by `voting_method` across
  the full date range will reflect at least one categorical scheme
  change. Analysts should treat `voting_method` as non-stationary over
  time, verify category definitions for the specific date range of
  interest, and independently assess deduplication strategy for
  `(ncid, election_lbl)` before treating row count as vote count.
- I have made a note to reprocess my archived files and recreate the table
  to include a `data_date` field similar to the nc_vreg_history table. Without
  having done any testing, my belief is that the addition of this field can be
  referenced in analysis to filter out any "duplicate" records that are using
  "outdated" descriptions.

## General note on philosophy

This pipeline prioritizes preserving NCSBE's data as reported, including
its inconsistencies, over inferring corrections. Where the state's export
process changes formatting, categorization, or coding conventions over
time, those changes are preserved as-is rather than retroactively
normalized. This means downstream analysis must account for these shifts
explicitly rather than assume a stable schema across the full historical
range.
