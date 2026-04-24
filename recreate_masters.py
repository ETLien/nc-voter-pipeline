"""
recreate_masters.py

Rebuilds nc_vreg_history and/or nc_vhis_history from the archived zip files in ARCHIVE_PATH.
Use this when the SQL tables need to be rebuilt from scratch after data loss or corruption.

Usage:
    python recreate_masters.py           # rebuild both tables
    python recreate_masters.py --vreg    # registration table only
    python recreate_masters.py --vhis    # history table only
"""

import argparse
import logging
import os
import zipfile
import pandas as pd
from sqlalchemy import create_engine

# config (matches nc_etl.py)

DB_USER = "your_postgres_username"
DB_PASSWORD = "your_postgres_password"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "your_database_name"
DB_SCHEMA = "public"

DB_PATH = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
VREG_TABLE = "nc_vreg_history"
VHIS_TABLE = "nc_vhis_history"
VREG_STAGING = "vreg_rebuild_staging"
VHIS_STAGING = "vhis_rebuild_staging"

ARCHIVE_PATH = r"C:\path\to\your\archive"
FILE_ENCODING = "latin-1"

CHUNK_SIZE = 500_000

VREG_STEM = "ncvoter_Statewide"
VHIS_STEM = "ncvhis_Statewide"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(SCRIPT_DIR, "recreate_masters.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


def normalize_whitespace(df):
    return df.apply(
        lambda col: col.map(lambda x: " ".join(x.split()) if isinstance(x, str) else x)
    )


def get_table_columns(engine, schema, table):
    with engine.connect() as conn:
        result = conn.exec_driver_sql(f'SELECT * FROM {schema}."{table}" LIMIT 0;')
        return [m[0] for m in result.cursor.description]


def get_archived_zips(archive_dir, stem):
    zips = [
        os.path.join(archive_dir, f)
        for f in os.listdir(archive_dir)
        if f.endswith(".zip") and stem in f
    ]
    zips.sort()
    return zips


def extract_date_from_filename(zip_path):
    return os.path.basename(zip_path)[:8]


def unzip_to_dir(zip_path, extract_dir):
    stem = os.path.basename(zip_path)[9:-4]
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)
    return os.path.join(extract_dir, f"{stem}.txt")


def confirm_truncate(tables):
    print("\nWARNING: This will TRUNCATE the following tables and rebuild from archive:")
    for t in tables:
        print(f"  - {t}")
    answer = input("\nType YES to continue: ").strip()
    return answer == "YES"


def drop_indexes(engine, table):
    with engine.begin() as conn:
        conn.exec_driver_sql(f"""
            DO $$
            DECLARE idx record;
            BEGIN
                FOR idx IN
                    SELECT indexname FROM pg_indexes
                    WHERE schemaname = '{DB_SCHEMA}'
                    AND tablename = '{table}'
                    AND indexname NOT LIKE '%_pkey'
                LOOP
                    EXECUTE 'DROP INDEX IF EXISTS ' || idx.indexname;
                END LOOP;
            END $$;
        """)
    log.info(f"Indexes dropped on {table}.")


def recreate_indexes(engine):
    log.info("Recreating indexes...")
    with engine.begin() as conn:
        conn.exec_driver_sql(f"""
            CREATE INDEX IF NOT EXISTS idx_{VREG_TABLE}_ncid_date
            ON {DB_SCHEMA}."{VREG_TABLE}" (ncid, data_date DESC);
        """)
        conn.exec_driver_sql(f"""
            CREATE INDEX IF NOT EXISTS idx_{VHIS_TABLE}_ncid_election
            ON {DB_SCHEMA}."{VHIS_TABLE}" (ncid, election_lbl);
        """)
    log.info("Indexes recreated.")


def rebuild_registration(engine):
    log.info(f"Starting rebuild of {VREG_TABLE}...")
    zips = get_archived_zips(ARCHIVE_PATH, VREG_STEM)
    if not zips:
        log.error(f"No archived {VREG_STEM} zips found in {ARCHIVE_PATH}. Aborting.")
        return

    log.info(f"Found {len(zips)} archived registration files.")

    drop_indexes(engine, VREG_TABLE)

    with engine.begin() as conn:
        conn.exec_driver_sql(f'TRUNCATE TABLE {DB_SCHEMA}."{VREG_TABLE}";')
    log.info(f"Truncated {VREG_TABLE}.")

    master_cols = get_table_columns(engine, DB_SCHEMA, VREG_TABLE)
    compare_cols = [c for c in master_cols if c != "data_date"]
    insert_cols = ", ".join([f'"{c}"' for c in master_cols])
    select_cols = ", ".join([f's."{c}"' for c in master_cols])
    latest_cols = ", ".join([f'"{c}"' for c in master_cols])
    match_conditions = " AND ".join([f'latest."{c}" = s."{c}"' for c in compare_cols])

    for i, zip_path in enumerate(zips, start=1):
        date_str = extract_date_from_filename(zip_path)
        zip_name = os.path.basename(zip_path)
        log.info(f"[{i}/{len(zips)}] {zip_name} ({date_str})")

        txt_path = unzip_to_dir(zip_path, ARCHIVE_PATH)

        with engine.begin() as conn:
            conn.exec_driver_sql(f'DROP TABLE IF EXISTS {DB_SCHEMA}."{VREG_STAGING}";')
            conn.exec_driver_sql(f"""
                CREATE UNLOGGED TABLE {DB_SCHEMA}."{VREG_STAGING}"
                (LIKE {DB_SCHEMA}."{VREG_TABLE}");
            """)

        total_staged = 0
        for chunk in pd.read_csv(
            txt_path, sep="\t", dtype="str", na_filter=False,
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

        log.info(f"  Staged {total_staged:,} rows. Running anti-join insert...")

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
            conn.exec_driver_sql(f'DROP TABLE {DB_SCHEMA}."{VREG_STAGING}";')

        os.remove(txt_path)
        log.info(f"  Inserted {result.rowcount:,} new/changed records.")

    log.info(f"Registration rebuild complete.")


def rebuild_history(engine):
    log.info(f"Starting rebuild of {VHIS_TABLE}...")
    zips = get_archived_zips(ARCHIVE_PATH, VHIS_STEM)
    if not zips:
        log.error(f"No archived {VHIS_STEM} zips found in {ARCHIVE_PATH}. Aborting.")
        return

    log.info(f"Found {len(zips)} archived history files.")

    drop_indexes(engine, VHIS_TABLE)

    with engine.begin() as conn:
        conn.exec_driver_sql(f'TRUNCATE TABLE {DB_SCHEMA}."{VHIS_TABLE}";')
    log.info(f"Truncated {VHIS_TABLE}.")

    master_cols = get_table_columns(engine, DB_SCHEMA, VHIS_TABLE)
    quoted_cols = ", ".join([f'"{c}"' for c in master_cols])
    select_cols = ", ".join([f's."{c}"' for c in master_cols])
    s_hash = " || '||' || ".join([f"COALESCE(s.\"{c}\"::text, '')" for c in master_cols])
    h_hash = " || '||' || ".join([f"COALESCE(h.\"{c}\"::text, '')" for c in master_cols])

    for i, zip_path in enumerate(zips, start=1):
        date_str = extract_date_from_filename(zip_path)
        zip_name = os.path.basename(zip_path)
        log.info(f"[{i}/{len(zips)}] {zip_name} ({date_str})")

        txt_path = unzip_to_dir(zip_path, ARCHIVE_PATH)

        with engine.begin() as conn:
            conn.exec_driver_sql(f'DROP TABLE IF EXISTS {DB_SCHEMA}."{VHIS_STAGING}";')
            conn.exec_driver_sql(f"""
                CREATE UNLOGGED TABLE {DB_SCHEMA}."{VHIS_STAGING}"
                (LIKE {DB_SCHEMA}."{VHIS_TABLE}");
            """)

        total_staged = 0
        chunk_num = 0
        for chunk in pd.read_csv(
            txt_path, sep="\t", dtype="str", na_filter=False,
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
                chunk.to_sql(VHIS_STAGING, schema=DB_SCHEMA, con=conn, if_exists="append", index=False)
            total_staged += len(chunk)

        log.info(f"  Staged {total_staged:,} rows. Running anti-join insert...")

        with engine.begin() as conn:
            result = conn.exec_driver_sql(f"""
                INSERT INTO {DB_SCHEMA}."{VHIS_TABLE}" ({quoted_cols})
                SELECT {select_cols}
                FROM {DB_SCHEMA}."{VHIS_STAGING}" s
                WHERE NOT EXISTS (
                    SELECT 1 FROM {DB_SCHEMA}."{VHIS_TABLE}" h
                    WHERE md5({h_hash}) = md5({s_hash})
                );
            """)
            conn.exec_driver_sql(f'DROP TABLE {DB_SCHEMA}."{VHIS_STAGING}";')

        os.remove(txt_path)
        log.info(f"  Inserted {result.rowcount:,} new records.")

    log.info(f"History rebuild complete.")


def main():
    parser = argparse.ArgumentParser(
        description="Rebuild NC voter master tables from archived zip files."
    )
    parser.add_argument("--vreg", action="store_true", help="Rebuild registration table only")
    parser.add_argument("--vhis", action="store_true", help="Rebuild history table only")
    args = parser.parse_args()

    run_vreg = args.vreg or (not args.vreg and not args.vhis)
    run_vhis = args.vhis or (not args.vreg and not args.vhis)

    tables_to_truncate = []
    if run_vreg:
        tables_to_truncate.append(VREG_TABLE)
    if run_vhis:
        tables_to_truncate.append(VHIS_TABLE)

    if not confirm_truncate(tables_to_truncate):
        print("Aborted.")
        return

    engine = create_engine(DB_PATH, pool_pre_ping=True)

    if run_vreg:
        rebuild_registration(engine)
    if run_vhis:
        rebuild_history(engine)

    recreate_indexes(engine)
    log.info("Done.")


if __name__ == "__main__":
    main()
