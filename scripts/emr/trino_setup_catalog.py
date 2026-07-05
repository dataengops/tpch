"""Register the 8 SF400 TPC-H parquet tables in AWS Glue as Trino external
tables. DDL column types are derived from each parquet file's footer (read from
S3 via pyarrow), so the schema matches whatever generated the data. Runs on the
EMR primary node against the local Trino coordinator.
"""

from __future__ import annotations

import argparse
import time

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs

TABLES = [
    "customer", "lineitem", "nation", "orders",
    "part", "partsupp", "region", "supplier",
]


def arrow_to_trino(dtype: pa.DataType) -> str:
    if pa.types.is_int64(dtype):
        return "bigint"
    if pa.types.is_int32(dtype):
        return "integer"
    if pa.types.is_int16(dtype):
        return "smallint"
    if pa.types.is_int8(dtype):
        return "tinyint"
    if pa.types.is_float64(dtype):
        return "double"
    if pa.types.is_float32(dtype):
        return "real"
    if pa.types.is_decimal(dtype):
        return f"decimal({dtype.precision}, {dtype.scale})"
    if pa.types.is_date(dtype):
        return "date"
    if pa.types.is_timestamp(dtype):
        return "timestamp"
    if pa.types.is_boolean(dtype):
        return "boolean"
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return "varchar"
    raise ValueError(f"unmapped arrow type: {dtype}")


def build_create_table_sql(
    table: str, schema: pa.Schema, catalog: str, db: str, location: str
) -> str:
    cols = ",\n    ".join(
        f"{field.name} {arrow_to_trino(field.type)}" for field in schema
    )
    return (
        f"create table {catalog}.{db}.{table} (\n    {cols}\n) with (\n"
        f"    external_location = '{location}',\n"
        f"    format = 'PARQUET'\n)"
    )


def read_parquet_schema(s3: fs.S3FileSystem, bucket: str, key: str) -> pa.Schema:
    with s3.open_input_file(f"{bucket}/{key}") as f:
        return pq.read_schema(f)


def connect_with_retry(host: str, port: int, catalog: str, attempts: int = 30):
    import trino  # imported here so the unit test needs no trino install

    last = None
    for _ in range(attempts):
        try:
            conn = trino.dbapi.connect(
                host=host, port=port, user="hadoop", catalog=catalog,
            )
            conn.cursor().execute("select 1").fetchall()
            return conn
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(10)
    raise RuntimeError(f"Trino not reachable at {host}:{port}: {last}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--data-prefix", required=True,
                    help="e.g. scale-400.0/trino (per-table dirs beneath)")
    ap.add_argument("--schema", required=True, help="Glue/Hive schema (database) name")
    ap.add_argument("--region", required=True)
    ap.add_argument("--catalog", default="hive")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=8889)
    ap.add_argument(
        "--location-base", default="",
        help="Base URI for table data, e.g. hdfs:///tpch/sf400. The schema is "
             "still read from the S3 parquet footer (bucket/data-prefix), but the "
             "table external_location points here. Empty = use the S3 prefix.",
    )
    args = ap.parse_args()

    s3 = fs.S3FileSystem(region=args.region)
    prefix = args.data_prefix.strip("/")
    loc_base = args.location_base.rstrip("/") or f"s3://{args.bucket}/{prefix}"

    conn = connect_with_retry(args.host, args.port, args.catalog)
    cur = conn.cursor()

    schema_loc = f"{loc_base}/"
    cur.execute(
        f"create schema if not exists {args.catalog}.{args.schema} "
        f"with (location = '{schema_loc}')"
    ).fetchall()

    for t in TABLES:
        key = f"{prefix}/{t}/{t}.parquet"
        arrow_schema = read_parquet_schema(s3, args.bucket, key)
        location = f"{loc_base}/{t}/"
        cur.execute(
            f"drop table if exists {args.catalog}.{args.schema}.{t}"
        ).fetchall()
        ddl = build_create_table_sql(
            t, arrow_schema, args.catalog, args.schema, location
        )
        print(f"creating {t}:\n{ddl}\n")
        cur.execute(ddl).fetchall()

    print(f"Registered {len(TABLES)} tables in {args.catalog}.{args.schema}.")


if __name__ == "__main__":
    main()
