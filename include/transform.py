"""Transformation logic used by q4_fixed.
"""
from __future__ import annotations

import os
from pathlib import Path

import duckdb


def transform_to_summary(
    parquet_path: str,
    summary_path: str,
    min_amount: float = 0.0,
) -> dict:
    """Filter, group, and write a per-(category, payment_method) summary.

    Reads `parquet_path`, drops rows with amount <= `min_amount`, groups by
    category and payment_method, writes the result to `summary_path` (overwrite).

    Returns metadata only -- never the data itself. This is the correct XCom payload.
    """
    if not os.path.exists(parquet_path):
        raise FileNotFoundError(f"Input not found: {parquet_path}")

    Path(summary_path).parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    try:
        con.execute(
            f"""
            COPY (
                SELECT
                    category,
                    payment_method,
                    COUNT(*) AS tx_count,
                    SUM(amount_eur) AS revenue,
                    AVG(amount_eur) AS avg_ticket
                FROM read_parquet('{parquet_path}')
                WHERE amount_eur > {min_amount}
                GROUP BY category, payment_method
                ORDER BY revenue DESC
            ) TO '{summary_path}' (FORMAT PARQUET)
            """
        )
        n_groups = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{summary_path}')"
        ).fetchone()[0]
    finally:
        con.close()

    return {"summary_path": summary_path, "group_count": int(n_groups)}
