"""Offline verification of the Trino TPC-H suite.

- sqlglot parses every statement in the Trino dialect (syntax).
- duckdb EXPLAINs every statement against local SF100 in datain/ (column/type
  binding, no execution). Run from the repo root with the repo venv:
    .venv/Scripts/python.exe scripts/emr/tests/test_trino_queries.py
"""

from __future__ import annotations

import pathlib
import sys

import duckdb
import sqlglot

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from trino_queries import QUERIES  # noqa: E402

TABLES = [
    "customer", "lineitem", "nation", "orders",
    "part", "partsupp", "region", "supplier",
]
DATAIN = pathlib.Path(__file__).resolve().parents[3] / "datain"


def main() -> None:
    assert sorted(QUERIES) == list(range(1, 23)), f"expected q1..q22, got {sorted(QUERIES)}"

    # 1. Trino-dialect syntax check.
    for n, sql in QUERIES.items():
        sqlglot.parse_one(sql, dialect="trino")  # raises on invalid Trino SQL

    # 2. Column/type binding against the real SF100 schema (EXPLAIN, no run).
    con = duckdb.connect()
    for t in TABLES:
        p = (DATAIN / f"{t}.parquet").as_posix()
        con.execute(f"create view {t} as select * from read_parquet('{p}')")
    for n, sql in QUERIES.items():
        try:
            con.execute("EXPLAIN " + sql).fetchall()
        except Exception as e:  # noqa: BLE001
            raise AssertionError(f"q{n} failed to bind: {e}") from e

    print(f"OK: {len(QUERIES)} statements parsed (trino) and bound (duckdb).")


if __name__ == "__main__":
    main()
