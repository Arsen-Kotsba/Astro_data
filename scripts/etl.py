import hashlib
import os
from datetime import datetime
from io import StringIO
from urllib.parse import quote_plus

import pandas as pd
import requests
from sqlalchemy import create_engine, text

EXOPLANET_TAP_BASE_URL = "https://exoplanetarchive.ipac.caltech.edu/TAP/sync"
EXOPLANET_SELECT_COLUMNS = [
    "pl_name",
    "hostname",
    "discoverymethod",
    "disc_year",
    "disc_pubdate",
    "pl_orbper",
    "pl_rade",
    "pl_bmasse",
    "st_teff",
    "sy_dist",
]


def build_exoplanet_query(last_disc_pubdate: datetime | None) -> str:
    column_sql = ", ".join(EXOPLANET_SELECT_COLUMNS)
    where = ""
    if last_disc_pubdate is not None:
        cutoff = pd.to_datetime(last_disc_pubdate).strftime("%Y-%m-%d")
        where = f" where disc_pubdate > '{cutoff}'"

    return f"select {column_sql} from pscomppars{where}"


def extract_exoplanets(last_disc_pubdate: datetime | None) -> pd.DataFrame:
    query = build_exoplanet_query(last_disc_pubdate)
    url = f"{EXOPLANET_TAP_BASE_URL}?query={quote_plus(query)}&format=csv"

    response = requests.get(url, timeout=60)
    response.raise_for_status()

    if not response.text.strip():
        return pd.DataFrame(columns=EXOPLANET_SELECT_COLUMNS)

    return pd.read_csv(StringIO(response.text))


def transform_exoplanets(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    result = df.copy()
    result.columns = [c.lower() for c in result.columns]

    result["disc_pubdate"] = pd.to_datetime(result["disc_pubdate"], errors="coerce")
    result["disc_year"] = pd.to_numeric(result["disc_year"], errors="coerce").astype("Int64")

    for col in ["pl_orbper", "pl_rade", "pl_bmasse", "st_teff", "sy_dist"]:
        result[col] = pd.to_numeric(result[col], errors="coerce")

    result = result[result["pl_name"].notna()].copy()
    result["pl_name"] = result["pl_name"].astype(str).str.strip()
    result = result[result["pl_name"] != ""]

    result = result.drop_duplicates(subset=["pl_name", "disc_pubdate"], keep="last")
    result["source_row_hash"] = result.apply(make_source_row_hash, axis=1)
    result["load_dttm"] = datetime.utcnow()

    return result


def make_source_row_hash(row: pd.Series) -> str:
    fields = [
        row.get("pl_name"),
        row.get("hostname"),
        row.get("discoverymethod"),
        row.get("disc_year"),
        row.get("disc_pubdate"),
        row.get("pl_orbper"),
        row.get("pl_rade"),
        row.get("pl_bmasse"),
        row.get("st_teff"),
        row.get("sy_dist"),
    ]
    payload = "|".join("" if pd.isna(v) else str(v) for v in fields)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def get_engine():
    server = os.getenv("MSSQL_SERVER", "localhost")
    port = os.getenv("MSSQL_PORT", "1433")
    database = os.getenv("MSSQL_DATABASE", "market_data")
    username = os.getenv("MSSQL_USERNAME", "sa")
    password = os.getenv("MSSQL_PASSWORD", "CiiCCS24=PN")
    driver = os.getenv("MSSQL_DRIVER", "ODBC Driver 17 for SQL Server")

    connection_string = (
        f"mssql+pyodbc://{username}:{quote_plus(password)}@{server}:{port}/{database}"
        f"?driver={quote_plus(driver)}"
    )
    return create_engine(connection_string)


def get_last_disc_pubdate(engine):
    query = text("SELECT MAX(disc_pubdate) AS last_disc_pubdate FROM dim_exoplanets")
    with engine.connect() as conn:
        row = conn.execute(query).fetchone()
    return row[0] if row and row[0] else None


def load_staging(df: pd.DataFrame, engine, source_run_id: int):
    if df.empty:
        return 0

    stage_df = df.copy()
    stage_df["source_run_id"] = source_run_id
    stage_df.to_sql("stg_exoplanets_raw", engine, if_exists="append", index=False)
    return len(stage_df)


def merge_to_core(engine, source_run_id: int):
    merge_query = text(
        """
        MERGE dim_exoplanets AS target
        USING (
            SELECT
                pl_name,
                hostname,
                discoverymethod,
                disc_year,
                disc_pubdate,
                pl_orbper,
                pl_rade,
                pl_bmasse,
                st_teff,
                sy_dist,
                source_row_hash
            FROM stg_exoplanets_raw
            WHERE source_run_id = :source_run_id
        ) AS src
        ON target.pl_name = src.pl_name
        WHEN MATCHED AND target.source_row_hash <> src.source_row_hash THEN
            UPDATE SET
                target.hostname = src.hostname,
                target.discoverymethod = src.discoverymethod,
                target.disc_year = src.disc_year,
                target.disc_pubdate = src.disc_pubdate,
                target.pl_orbper = src.pl_orbper,
                target.pl_rade = src.pl_rade,
                target.pl_bmasse = src.pl_bmasse,
                target.st_teff = src.st_teff,
                target.sy_dist = src.sy_dist,
                target.source_row_hash = src.source_row_hash,
                target.last_seen_dttm = SYSUTCDATETIME(),
                target.is_current = 1
        WHEN NOT MATCHED THEN
            INSERT (
                pl_name, hostname, discoverymethod, disc_year, disc_pubdate,
                pl_orbper, pl_rade, pl_bmasse, st_teff, sy_dist, source_row_hash,
                first_seen_dttm, last_seen_dttm, is_current
            )
            VALUES (
                src.pl_name, src.hostname, src.discoverymethod, src.disc_year, src.disc_pubdate,
                src.pl_orbper, src.pl_rade, src.pl_bmasse, src.st_teff, src.sy_dist, src.source_row_hash,
                SYSUTCDATETIME(), SYSUTCDATETIME(), 1
            );
        """
    )
    with engine.begin() as conn:
        conn.execute(merge_query, {"source_run_id": source_run_id})


def start_log(engine, entity_name: str, watermark_before):
    query = text(
        """
        INSERT INTO etl_log (entity_name, start_time, status, watermark_before)
        OUTPUT INSERTED.id
        VALUES (:entity_name, :start_time, :status, :watermark_before)
        """
    )
    with engine.begin() as conn:
        result = conn.execute(
            query,
            {
                "entity_name": entity_name,
                "start_time": datetime.utcnow(),
                "status": "RUNNING",
                "watermark_before": watermark_before,
            },
        )
        return result.fetchone()[0]


def success_log(engine, log_id: int, rows_staged: int, watermark_after):
    query = text(
        """
        UPDATE etl_log
        SET end_time = :end_time,
            status = 'SUCCESS',
            rows_loaded = :rows_loaded,
            watermark_after = :watermark_after
        WHERE id = :id
        """
    )
    with engine.begin() as conn:
        conn.execute(
            query,
            {
                "end_time": datetime.utcnow(),
                "rows_loaded": rows_staged,
                "watermark_after": watermark_after,
                "id": log_id,
            },
        )


def error_log(engine, log_id: int, error_message: Exception):
    query = text(
        """
        UPDATE etl_log
        SET end_time = :end_time,
            status = 'FAILED',
            error_message = :error_message
        WHERE id = :id
        """
    )
    with engine.begin() as conn:
        conn.execute(
            query,
            {
                "end_time": datetime.utcnow(),
                "error_message": str(error_message)[:4000],
                "id": log_id,
            },
        )


def run_etl():
    engine = get_engine()
    watermark_before = get_last_disc_pubdate(engine)
    log_id = start_log(engine, "exoplanets", watermark_before)

    try:
        raw_df = extract_exoplanets(watermark_before)
        df = transform_exoplanets(raw_df)

        if df.empty:
            success_log(engine, log_id, rows_staged=0, watermark_after=watermark_before)
            print("No new exoplanet rows to process.")
            return

        rows_staged = load_staging(df, engine, source_run_id=log_id)
        merge_to_core(engine, source_run_id=log_id)
        watermark_after = df["disc_pubdate"].max()

        success_log(engine, log_id, rows_staged=rows_staged, watermark_after=watermark_after)
        print(f"Staged and merged {rows_staged} exoplanet rows.")
    except Exception as exc:
        error_log(engine, log_id, exc)
        raise


if __name__ == "__main__":
    run_etl()
