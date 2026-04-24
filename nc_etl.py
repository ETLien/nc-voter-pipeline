"""
NC Voter Data ETL

Automated hourly pipeline for the NC State Board of Elections voter data.
Checks S3 for updated files, downloads and processes changes, and appends
new/changed records to a local PostgreSQL database.

To run: python nc_etl.py
To install dependencies: pip install pandas "psycopg[binary]" sqlalchemy schedule

Before running, fill in the config section below with your own values.
"""

import json
import logging
import os
import shutil
import time
import urllib.error
import urllib.request
import zipfile
from datetime import datetime
from email.utils import parsedate_to_datetime
import pandas as pd
import psycopg
import schedule
from sqlalchemy import create_engine

VREG_URL = "https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvoter_Statewide.zip"
VHIS_URL = "https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvhis_Statewide.zip"

FILE_ENCODING = "latin-1"
CHUNK_SIZE = 100_000

VREG_STEM = "ncvoter_Statewide"
VHIS_STEM = "ncvhis_Statewide"

DB_USER = "your_postgres_username"
DB_PASSWORD = "your_postgres_password"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "your_database_name"
DB_SCHEMA = "public"

DB_PATH = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
VREG_TABLE = "nc_vreg_history"
VHIS_TABLE = "nc_vhis_history"
VREG_STAGING = "vreg_staging"

DOWNLOAD_PATH = r"C:\path\to\your\downloads"
ARCHIVE_PATH = r"C:\path\to\your\archive"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_FILE = os.path.join(SCRIPT_DIR, "last_modified.json")
LOG_FILE = os.path.join(SCRIPT_DIR, "nc_etl.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

def main():
    log.info("NC ETL starting. Running immediately, then checking every hour.")
    run_pipeline()
    schedule.every(1).hour.do(run_pipeline)
    while True:
        schedule.run_pending()
        time.sleep(60)


def run_pipeline():
    log.info("--- Hourly check starting ---")
    try:
        updates = check_for_updates(CACHE_FILE)
        if not updates:
            log.info("No updates detected. Skipping.")
            return

        for url, (raw, date_str) in updates.items():
            log.info(f"Update detected: {os.path.basename(url)} — {raw}")

        copy_sql_tables_to_archive(DB_PATH, [VREG_TABLE, VHIS_TABLE], ARCHIVE_PATH)

        for url, (raw, date_str) in updates.items():
            filename = os.path.basename(url)
            stem = filename.split(".")[0]

            save_file_status(CACHE_FILE, filename, raw, processed=False)

            download_file(url, DOWNLOAD_PATH, ARCHIVE_PATH, date_str)
            if stem == VREG_STEM:
                process_registration_file(
                    os.path.join(DOWNLOAD_PATH, f"{stem}.txt"), date_str
                )
            elif stem == VHIS_STEM:
                process_history_file(
                    os.path.join(DOWNLOAD_PATH, f"{stem}.txt"), date_str
                )

            save_file_status(CACHE_FILE, filename, raw, processed=True)

        log.info("--- Pipeline complete ---")

    except Exception:
        log.error("Pipeline failed. See cache for per-file status; next run will retry unfinished files.", exc_info=True)

def check_for_updates(cache_file):
    cached = load_cached_timestamps(cache_file)
    updates = {}
    for url in [VREG_URL, VHIS_URL]:
        raw, date_str = fetch_last_modified(url)
        if raw is None:
            continue
        filename = os.path.basename(url)
        entry = cached.get(filename, {})
        cached_timestamp = entry.get("timestamp")
        processed = entry.get("processed", True)
        if cached_timestamp != raw or not processed:
            updates[url] = (raw, date_str)
    return updates


def fetch_last_modified(url):
    try:
        req = urllib.request.Request(url, method="HEAD")
        with urllib.request.urlopen(req, timeout=30) as resp:
            header = resp.headers.get("Last-Modified")
    except urllib.error.HTTPError as e:
        log.warning(f"S3 returned HTTP {e.code} for {url} — skipping this check.")
        return None, None
    except urllib.error.URLError as e:
        log.warning(f"Could not reach S3 ({e.reason}) — skipping this check.")
        return None, None
    except Exception as e:
        log.warning(f"Unexpected error checking S3: {e} — skipping this check.")
        return None, None

    if not header:
        log.warning(f"No Last-Modified header returned for {url}")
        return None, None
    try:
        date_str = parsedate_to_datetime(header).strftime("%Y%m%d")
        return header, date_str
    except Exception:
        log.warning(f"Could not parse Last-Modified value: {header!r}")
        return None, None


def load_cached_timestamps(cache_file):
    if not os.path.exists(cache_file):
        return {}
    try:
        with open(cache_file) as f:
            data = json.load(f)
        return {
            k: v if isinstance(v, dict) else {"timestamp": v, "processed": True}
            for k, v in data.items()
        }
    except Exception:
        return {}


def save_file_status(cache_file, filename, raw_timestamp, processed):
    existing = load_cached_timestamps(cache_file)
    existing[filename] = {"timestamp": raw_timestamp, "processed": processed}
    with open(cache_file, "w") as f:
        json.dump(existing, f, indent=2)

def copy_sql_tables_to_archive(db_path, table_names, destination):
    for table in table_names:
        out_path = os.path.join(destination, f"{table}.tsv")
        log.info(f"Archiving {table} → {out_path}")
        with psycopg.connect(db_path) as conn:
            with conn.cursor() as cur, open(out_path, "wb") as outfile:
                cur.execute("START TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;")
                copy_sql = (
                    f'COPY "{table}" TO STDOUT '
                    f"WITH (FORMAT csv, DELIMITER E'\\t', HEADER true, ENCODING 'UTF8')"
                )
                with cur.copy(copy_sql) as cp:
                    for chunk in cp:
                        outfile.write(chunk)
        log.info(f"Archived {table}.")

def download_file(url, download_dir, archive_dir, date_str):
    zip_name = os.path.basename(url)
    zip_dest = os.path.join(download_dir, zip_name)
    log.info(f"Downloading {zip_name}...")
    try:
        urllib.request.urlretrieve(url, zip_dest)
    except urllib.error.HTTPError as e:
        log.error(f"HTTP {e.code} downloading {url}")
        raise
    except urllib.error.URLError as e:
        log.error(f"Network error downloading {url}: {e.reason}")
        raise
    copy_to_archive(zip_dest, archive_dir, date_str)
    unzip(zip_dest, download_dir)
    os.remove(zip_dest)


def copy_to_archive(zip_path, archive_dir, date_str):
    name = os.path.basename(zip_path)
    dest = os.path.join(archive_dir, f"{date_str}_{name}")
    shutil.copy(zip_path, dest)
    log.info(f"Archived download as {os.path.basename(dest)}")


def unzip(zip_path, extract_dir):
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)
    log.info(f"Unzipped {os.path.basename(zip_path)}")

def normalize_whitespace(df):
    return df.apply(
        lambda col: col.map(lambda x: " ".join(x.split()) if isinstance(x, str) else x)
    )


def get_table_columns(engine, schema, table):
    with engine.connect() as conn:
        result = conn.exec_driver_sql(f'SELECT * FROM {schema}."{table}" LIMIT 0;')
        return [m[0] for m in result.cursor.description]


def ensure_indexes(engine):
    with engine.begin() as conn:
        conn.exec_driver_sql(f"""
            CREATE INDEX IF NOT EXISTS idx_{VREG_TABLE}_ncid_date
            ON {DB_SCHEMA}."{VREG_TABLE}" (ncid, data_date DESC);
        """)
        conn.exec_driver_sql(f"""
            CREATE INDEX IF NOT EXISTS idx_{VHIS_TABLE}_ncid_election
            ON {DB_SCHEMA}."{VHIS_TABLE}" (ncid, election_lbl);
        """)
    log.info("Indexes verified.")

def process_registration_file(file_path, date_str):
    log.info(f"Processing registration file: {file_path}")
    engine = create_engine(DB_PATH, pool_pre_ping=True)
    ensure_indexes(engine)
    master_cols = get_table_columns(engine, DB_SCHEMA, VREG_TABLE)

    with engine.begin() as conn:
        conn.exec_driver_sql(f'DROP TABLE IF EXISTS {DB_SCHEMA}."{VREG_STAGING}";')
        conn.exec_driver_sql(f"""
            CREATE TABLE {DB_SCHEMA}."{VREG_STAGING}"
            (LIKE {DB_SCHEMA}."{VREG_TABLE}");
        """)
    log.info("Staging table created.")

    total_staged = 0
    for chunk in pd.read_csv(
        file_path, sep="\t", dtype="str", na_filter=False,
        encoding=FILE_ENCODING, chunksize=CHUNK_SIZE
    ):
        chunk = normalize_whitespace(chunk)
        chunk["data_date"] = date_str
        for col in master_cols:
            if col not in chunk.columns:
                chunk[col] = None
        chunk = chunk[master_cols]
        with engine.begin() as conn:
            chunk.to_sql(VREG_STAGING, schema=DB_SCHEMA, con=conn, if_exists="append", index=False)
        total_staged += len(chunk)
    log.info(f"Staged {total_staged:,} rows.")

    compare_cols = [c for c in master_cols if c != "data_date"]
    insert_cols = ", ".join([f'"{c}"' for c in master_cols])
    select_cols = ", ".join([f's."{c}"' for c in master_cols])
    latest_cols = ", ".join([f'"{c}"' for c in master_cols])
    match_conditions = " AND ".join([f'latest."{c}" = s."{c}"' for c in compare_cols])

    with engine.begin() as conn:
        result = conn.exec_driver_sql(f"""
            INSERT INTO {DB_SCHEMA}."{VREG_TABLE}" ({insert_cols})
            SELECT {select_cols}
            FROM {DB_SCHEMA}."{VREG_STAGING}" s
            WHERE NOT EXISTS (
                SELECT 1
                FROM (
                    SELECT DISTINCT ON (ncid) {latest_cols}
                    FROM {DB_SCHEMA}."{VREG_TABLE}"
                    ORDER BY ncid, data_date DESC
                ) latest
                WHERE {match_conditions}
            );
        """)
        rows_inserted = result.rowcount
        conn.exec_driver_sql(f'DROP TABLE {DB_SCHEMA}."{VREG_STAGING}";')

    os.remove(file_path)
    log.info(f"Registration complete. {rows_inserted:,} new/changed records → {VREG_TABLE}")

def process_history_file(file_path, date_str):
    log.info(f"Processing history file: {file_path}")
    engine = create_engine(DB_PATH, pool_pre_ping=True)
    master_cols = get_table_columns(engine, DB_SCHEMA, VHIS_TABLE)

    quoted_cols = ", ".join([f'"{c}"' for c in master_cols])
    select_cols = ", ".join([f's."{c}"' for c in master_cols])
    s_hash = " || '||' || ".join([f"COALESCE(s.\"{c}\"::text, '')" for c in master_cols])
    h_hash = " || '||' || ".join([f"COALESCE(h.\"{c}\"::text, '')" for c in master_cols])

    total_new = 0
    chunk_num = 0
    for chunk in pd.read_csv(
        file_path, sep="\t", dtype="str", na_filter=False,
        encoding=FILE_ENCODING, chunksize=CHUNK_SIZE
    ):
        chunk_num += 1
        chunk = normalize_whitespace(chunk)
        chunk["election_lbl"] = pd.to_datetime(
            chunk["election_lbl"], format="%m/%d/%Y", errors="coerce"
        ).dt.date
        for col in master_cols:
            if col not in chunk.columns:
                chunk[col] = None
        chunk = chunk[master_cols]

        with engine.begin() as conn:
            conn.exec_driver_sql(f"""
                CREATE TEMP TABLE vhis_chunk
                (LIKE {DB_SCHEMA}."{VHIS_TABLE}")
                ON COMMIT DROP;
            """)
            chunk.to_sql("vhis_chunk", schema=None, con=conn, if_exists="append", index=False)
            result = conn.exec_driver_sql(f"""
                INSERT INTO {DB_SCHEMA}."{VHIS_TABLE}" ({quoted_cols})
                SELECT {select_cols}
                FROM vhis_chunk s
                WHERE NOT EXISTS (
                    SELECT 1 FROM {DB_SCHEMA}."{VHIS_TABLE}" h
                    WHERE md5({h_hash}) = md5({s_hash})
                );
            """)
            total_new += result.rowcount

        if chunk_num % 10 == 0:
            log.info(f"  ... processed {chunk_num * CHUNK_SIZE:,} rows so far")

    os.remove(file_path)
    log.info(f"History complete. {total_new:,} new records → {VHIS_TABLE}")


if __name__ == "__main__":
    main()
