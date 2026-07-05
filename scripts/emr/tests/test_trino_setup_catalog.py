"""Unit tests for the pure DDL-generation helpers in trino_setup_catalog.py.
Run: .venv/Scripts/python.exe scripts/emr/tests/test_trino_setup_catalog.py
"""

from __future__ import annotations

import pathlib
import sys

import pyarrow as pa

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from trino_setup_catalog import arrow_to_trino, build_create_table_sql  # noqa: E402


def test_type_map() -> None:
    assert arrow_to_trino(pa.int64()) == "bigint"
    assert arrow_to_trino(pa.int32()) == "integer"
    assert arrow_to_trino(pa.int16()) == "smallint"
    assert arrow_to_trino(pa.float64()) == "double"
    assert arrow_to_trino(pa.float32()) == "real"
    assert arrow_to_trino(pa.decimal128(15, 2)) == "decimal(15, 2)"
    assert arrow_to_trino(pa.date32()) == "date"
    assert arrow_to_trino(pa.string()) == "varchar"
    assert arrow_to_trino(pa.bool_()) == "boolean"


def test_build_ddl() -> None:
    schema = pa.schema([
        ("l_orderkey", pa.int64()),
        ("l_extendedprice", pa.decimal128(15, 2)),
        ("l_shipdate", pa.date32()),
        ("comments", pa.string()),
    ])
    sql = build_create_table_sql(
        "lineitem", schema, "hive", "tpch_sf400",
        "s3://b/scale-400.0/trino/lineitem/",
    )
    assert "create table hive.tpch_sf400.lineitem" in sql.lower()
    assert "l_orderkey bigint" in sql
    assert "l_extendedprice decimal(15, 2)" in sql
    assert "l_shipdate date" in sql
    assert "comments varchar" in sql
    assert "external_location = 's3://b/scale-400.0/trino/lineitem/'" in sql
    assert "format = 'PARQUET'" in sql


def main() -> None:
    test_type_map()
    test_build_ddl()
    print("OK: trino_setup_catalog helpers.")


if __name__ == "__main__":
    main()
