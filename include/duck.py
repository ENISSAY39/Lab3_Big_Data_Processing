"""DuckDB helpers for Lab 3.

Keep all data-manipulation logic in this module so DAG files stay
focused on orchestration (separation of concerns -- CM3).
"""
from __future__ import annotations

import os
from pathlib import Path

import duckdb


DEFAULT_INCOMING = "/opt/airflow/data/incoming"
DEFAULT_RAW = "/opt/airflow/data/raw"
DEFAULT_AGG = "/opt/airflow/data/agg"


def _ensure_parent(path: str | Path) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def csv_to_parquet(csv_path: str, parquet_path: str) -> dict:
    """Read a CSV with DuckDB and write it as Parquet (overwrite).

    Returns a small dict with row_count and amount_sum, suitable to pass
    through XComs (it stays well under a kilobyte).
    """
    _ensure_parent(parquet_path)

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Input CSV not found: {csv_path}")

    con = duckdb.connect()
    try:
        rel = con.read_csv(csv_path, header=True)
        rel.write_parquet(parquet_path)
        row_count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')").fetchone()[0]
        amount_sum = con.execute(
            f"SELECT COALESCE(SUM(amount_eur), 0) FROM read_parquet('{parquet_path}')"
        ).fetchone()[0]
    finally:
        con.close()

    return {
        "parquet_path": parquet_path,
        "row_count": int(row_count),
        "amount_sum": float(amount_sum),
    }


def validate_parquet(
    parquet_path: str,
    min_rows: int = 50,
    must_have_revenue: bool = True,
) -> dict:
    """Validate a Parquet file. Raise RuntimeError if invalid.

    Returns a small dict suitable for XCom transport.
    """
    if not os.path.exists(parquet_path):
        raise FileNotFoundError(f"Parquet not found: {parquet_path}")

    con = duckdb.connect()
    try:
        row_count = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')"
        ).fetchone()[0]
        amount_sum = con.execute(
            f"SELECT COALESCE(SUM(amount_eur), 0) FROM read_parquet('{parquet_path}')" # COALESCE returns the first non-null value, 0 if all are null
        ).fetchone()[0]
    finally:
        con.close()

    if row_count < min_rows:
        raise RuntimeError(
            f"Validation failed: row count {row_count} below minimum {min_rows}"
        )
    if must_have_revenue and amount_sum <= 0:
        raise RuntimeError(
            f"Validation failed: amount_sum is {amount_sum} -- input looks corrupt"
        )

    return {"row_count": int(row_count), "amount_sum": float(amount_sum)}


def daily_aggregate(parquet_path: str, agg_path: str) -> dict:
    """Aggregate daily revenue by product category and write to Parquet."""
    _ensure_parent(agg_path)

    con = duckdb.connect()
    try:
        con.execute(
            f"""
            COPY (
                SELECT
                    category,
                    COUNT(*) AS tx_count,
                    SUM(amount_eur) AS revenue
                FROM read_parquet('{parquet_path}')
                GROUP BY category
                ORDER BY revenue DESC
            ) TO '{agg_path}' (FORMAT PARQUET)
            """
        )
        n_categories = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{agg_path}')"
        ).fetchone()[0]
    finally:
        con.close()

    return {"agg_path": agg_path, "categories": int(n_categories)}
