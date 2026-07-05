"""Self-contained PySpark TPC-H (PDS) benchmark job for EMR-on-EC2.

Reads the 8 PDS/TPC-H tables as Parquet from S3, registers each as a temp view
once, then runs all 22 queries and prints per-query wall-clock timings. Designed
to be shipped as a single file to `spark-submit --deploy-mode cluster` on a
transient EMR cluster (see submit_emr_ec2.sh) -- no repo packaging required.

The query SQL is the same TPC-H suite used elsewhere in this repo
(see scripts/aws_emr.py and queries/pyspark/*).

Data layout (from `make data-tables` + `aws s3 sync`, non-partitioned default):
    s3://<bucket>/scale-100.0/lineitem.parquet
    s3://<bucket>/scale-100.0/orders.parquet
    ...
Point --data-path at the scale folder. Spark reads a single *.parquet file or a
directory of parquet parts transparently, so the partitioned layout works too
(pass --table-suffix "").

Disclaimer: portions derived from the TPC-H benchmark; results are not
comparable to published TPC-H results. See the repo README.
"""

from __future__ import annotations

import argparse
import time

from pyspark.sql import SparkSession

TABLES = [
    "customer",
    "lineitem",
    "nation",
    "orders",
    "part",
    "partsupp",
    "region",
    "supplier",
]

# Which tables each query touches -- used only for logging/clarity; all tables
# are registered up front so this is informational.
QUERIES: dict[int, str] = {
    1: """
        select
            l_returnflag,
            l_linestatus,
            sum(l_quantity) as sum_qty,
            sum(l_extendedprice) as sum_base_price,
            sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
            sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
            avg(l_quantity) as avg_qty,
            avg(l_extendedprice) as avg_price,
            avg(l_discount) as avg_disc,
            count(*) as count_order
        from lineitem
        where date(l_shipdate) <= date('1998-09-02')
        group by l_returnflag, l_linestatus
        order by l_returnflag, l_linestatus
    """,
    2: """
        select
            s_acctbal, s_name, n_name, p_partkey, p_mfgr,
            s_address, s_phone, s_comment
        from part, supplier, partsupp, nation, region
        where
            p_partkey = ps_partkey
            and s_suppkey = ps_suppkey
            and p_size = 15
            and p_type like '%BRASS'
            and s_nationkey = n_nationkey
            and n_regionkey = r_regionkey
            and r_name = 'EUROPE'
            and ps_supplycost = (
                select min(ps_supplycost)
                from partsupp, supplier, nation, region
                where
                    p_partkey = ps_partkey
                    and s_suppkey = ps_suppkey
                    and s_nationkey = n_nationkey
                    and n_regionkey = r_regionkey
                    and r_name = 'EUROPE'
            )
        order by s_acctbal desc, n_name, s_name, p_partkey
        limit 100
    """,
    3: """
        select
            l_orderkey,
            sum(l_extendedprice * (1 - l_discount)) as revenue,
            o_orderdate,
            o_shippriority
        from customer, orders, lineitem
        where
            c_mktsegment = 'BUILDING'
            and c_custkey = o_custkey
            and l_orderkey = o_orderkey
            and o_orderdate < date '1995-03-15'
            and l_shipdate > date '1995-03-15'
        group by l_orderkey, o_orderdate, o_shippriority
        order by revenue desc, o_orderdate
        limit 10
    """,
    4: """
        select o_orderpriority, count(*) as order_count
        from orders
        where
            o_orderdate >= date '1993-07-01'
            and o_orderdate < date '1993-07-01' + interval '3' month
            and exists (
                select * from lineitem
                where l_orderkey = o_orderkey
                    and l_commitdate < l_receiptdate
            )
        group by o_orderpriority
        order by o_orderpriority
    """,
    5: """
        select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue
        from customer, orders, lineitem, supplier, nation, region
        where
            c_custkey = o_custkey
            and l_orderkey = o_orderkey
            and l_suppkey = s_suppkey
            and c_nationkey = s_nationkey
            and s_nationkey = n_nationkey
            and n_regionkey = r_regionkey
            and r_name = 'ASIA'
            and o_orderdate >= date '1994-01-01'
            and o_orderdate < date '1994-01-01' + interval '1' year
        group by n_name
        order by revenue desc
    """,
    6: """
        select sum(l_extendedprice * l_discount) as revenue
        from lineitem
        where
            l_shipdate >= date '1994-01-01'
            and l_shipdate < date '1994-01-01' + interval '1' year
            and l_discount between .06 - 0.01 and .06 + 0.01
            and l_quantity < 24
    """,
    7: """
        select supp_nation, cust_nation, l_year, sum(volume) as revenue
        from (
            select
                n1.n_name as supp_nation,
                n2.n_name as cust_nation,
                year(l_shipdate) as l_year,
                l_extendedprice * (1 - l_discount) as volume
            from supplier, lineitem, orders, customer, nation n1, nation n2
            where
                s_suppkey = l_suppkey
                and o_orderkey = l_orderkey
                and c_custkey = o_custkey
                and s_nationkey = n1.n_nationkey
                and c_nationkey = n2.n_nationkey
                and (
                    (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
                    or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
                )
                and l_shipdate between date '1995-01-01' and date '1996-12-31'
        ) as shipping
        group by supp_nation, cust_nation, l_year
        order by supp_nation, cust_nation, l_year
    """,
    8: """
        select
            o_year,
            round(sum(case when nation = 'BRAZIL' then volume else 0 end)
                  / sum(volume), 2) as mkt_share
        from (
            select
                extract(year from o_orderdate) as o_year,
                l_extendedprice * (1 - l_discount) as volume,
                n2.n_name as nation
            from part, supplier, lineitem, orders, customer,
                 nation n1, nation n2, region
            where
                p_partkey = l_partkey
                and s_suppkey = l_suppkey
                and l_orderkey = o_orderkey
                and o_custkey = c_custkey
                and c_nationkey = n1.n_nationkey
                and n1.n_regionkey = r_regionkey
                and r_name = 'AMERICA'
                and s_nationkey = n2.n_nationkey
                and o_orderdate between date '1995-01-01' and date '1996-12-31'
                and p_type = 'ECONOMY ANODIZED STEEL'
        ) as all_nations
        group by o_year
        order by o_year
    """,
    9: """
        select nation, o_year, round(sum(amount), 2) as sum_profit
        from (
            select
                n_name as nation,
                year(o_orderdate) as o_year,
                l_extendedprice * (1 - l_discount)
                    - ps_supplycost * l_quantity as amount
            from part, supplier, lineitem, partsupp, orders, nation
            where
                s_suppkey = l_suppkey
                and ps_suppkey = l_suppkey
                and ps_partkey = l_partkey
                and p_partkey = l_partkey
                and o_orderkey = l_orderkey
                and s_nationkey = n_nationkey
                and p_name like '%green%'
        ) as profit
        group by nation, o_year
        order by nation, o_year desc
    """,
    10: """
        select
            c_custkey, c_name,
            round(sum(l_extendedprice * (1 - l_discount)), 2) as revenue,
            c_acctbal, n_name, c_address, c_phone, c_comment
        from customer, orders, lineitem, nation
        where
            c_custkey = o_custkey
            and l_orderkey = o_orderkey
            and o_orderdate >= date '1993-10-01'
            and o_orderdate < date '1993-10-01' + interval '3' month
            and l_returnflag = 'R'
            and c_nationkey = n_nationkey
        group by c_custkey, c_name, c_acctbal, c_phone, n_name,
                 c_address, c_comment
        order by revenue desc
        limit 20
    """,
    11: """
        select ps_partkey, round(sum(ps_supplycost * ps_availqty), 2) as value
        from partsupp, supplier, nation
        where
            ps_suppkey = s_suppkey
            and s_nationkey = n_nationkey
            and n_name = 'GERMANY'
        group by ps_partkey having
            sum(ps_supplycost * ps_availqty) > (
                select sum(ps_supplycost * ps_availqty) * 0.0001
                from partsupp, supplier, nation
                where
                    ps_suppkey = s_suppkey
                    and s_nationkey = n_nationkey
                    and n_name = 'GERMANY'
            )
        order by value desc
    """,
    12: """
        select
            l_shipmode,
            sum(case when o_orderpriority = '1-URGENT'
                        or o_orderpriority = '2-HIGH' then 1 else 0 end)
                as high_line_count,
            sum(case when o_orderpriority <> '1-URGENT'
                        and o_orderpriority <> '2-HIGH' then 1 else 0 end)
                as low_line_count
        from orders, lineitem
        where
            o_orderkey = l_orderkey
            and l_shipmode in ('MAIL', 'SHIP')
            and l_commitdate < l_receiptdate
            and l_shipdate < l_commitdate
            and l_receiptdate >= date '1994-01-01'
            and l_receiptdate < date '1994-01-01' + interval '1' year
        group by l_shipmode
        order by l_shipmode
    """,
    13: """
        select c_count, count(*) as custdist
        from (
            select c_custkey, count(o_orderkey)
            from customer left outer join orders on
                c_custkey = o_custkey
                and o_comment not like '%special%requests%'
            group by c_custkey
        ) as c_orders (c_custkey, c_count)
        group by c_count
        order by custdist desc, c_count desc
    """,
    14: """
        select round(100.00 * sum(case when p_type like 'PROMO%'
                then l_extendedprice * (1 - l_discount) else 0 end)
            / sum(l_extendedprice * (1 - l_discount)), 2) as promo_revenue
        from lineitem, part
        where
            l_partkey = p_partkey
            and l_shipdate >= date '1995-09-01'
            and l_shipdate < date '1995-09-01' + interval '1' month
    """,
    15: """
        with revenue (supplier_no, total_revenue) as (
            select l_suppkey, sum(l_extendedprice * (1 - l_discount))
            from lineitem
            where
                l_shipdate >= date '1996-01-01'
                and l_shipdate < date '1996-01-01' + interval '3' month
            group by l_suppkey
        )
        select s_suppkey, s_name, s_address, s_phone, total_revenue
        from supplier, revenue
        where
            s_suppkey = supplier_no
            and total_revenue = (select max(total_revenue) from revenue)
        order by s_suppkey
    """,
    16: """
        select p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt
        from partsupp, part
        where
            p_partkey = ps_partkey
            and p_brand <> 'Brand#45'
            and p_type not like 'MEDIUM POLISHED%'
            and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
            and ps_suppkey not in (
                select s_suppkey from supplier
                where s_comment like '%Customer%Complaints%'
            )
        group by p_brand, p_type, p_size
        order by supplier_cnt desc, p_brand, p_type, p_size
    """,
    17: """
        select round(sum(l_extendedprice) / 7.0, 2) as avg_yearly
        from lineitem, part
        where
            p_partkey = l_partkey
            and p_brand = 'Brand#23'
            and p_container = 'MED BOX'
            and l_quantity < (
                select 0.2 * avg(l_quantity) from lineitem
                where l_partkey = p_partkey
            )
    """,
    18: """
        select
            c_name, c_custkey, o_orderkey,
            to_date(o_orderdate) as o_orderdat, o_totalprice,
            DOUBLE(sum(l_quantity)) as col6
        from customer, orders, lineitem
        where
            o_orderkey in (
                select l_orderkey from lineitem
                group by l_orderkey having sum(l_quantity) > 300
            )
            and c_custkey = o_custkey
            and o_orderkey = l_orderkey
        group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
        order by o_totalprice desc, o_orderdate
        limit 100
    """,
    19: """
        select round(sum(l_extendedprice * (1 - l_discount)), 2) as revenue
        from lineitem, part
        where
            (
                p_partkey = l_partkey and p_brand = 'Brand#12'
                and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                and l_quantity >= 1 and l_quantity <= 1 + 10
                and p_size between 1 and 5
                and l_shipmode in ('AIR', 'AIR REG')
                and l_shipinstruct = 'DELIVER IN PERSON'
            )
            or (
                p_partkey = l_partkey and p_brand = 'Brand#23'
                and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                and l_quantity >= 10 and l_quantity <= 20
                and p_size between 1 and 10
                and l_shipmode in ('AIR', 'AIR REG')
                and l_shipinstruct = 'DELIVER IN PERSON'
            )
            or (
                p_partkey = l_partkey and p_brand = 'Brand#34'
                and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                and l_quantity >= 20 and l_quantity <= 30
                and p_size between 1 and 15
                and l_shipmode in ('AIR', 'AIR REG')
                and l_shipinstruct = 'DELIVER IN PERSON'
            )
    """,
    20: """
        select s_name, s_address
        from supplier, nation
        where
            s_suppkey in (
                select ps_suppkey from partsupp
                where
                    ps_partkey in (
                        select p_partkey from part where p_name like 'forest%'
                    )
                    and ps_availqty > (
                        select 0.5 * sum(l_quantity) from lineitem
                        where
                            l_partkey = ps_partkey
                            and l_suppkey = ps_suppkey
                            and l_shipdate >= date '1994-01-01'
                            and l_shipdate < date '1994-01-01' + interval '1' year
                    )
            )
            and s_nationkey = n_nationkey
            and n_name = 'CANADA'
        order by s_name
    """,
    21: """
        select s_name, count(*) as numwait
        from supplier, lineitem l1, orders, nation
        where
            s_suppkey = l1.l_suppkey
            and o_orderkey = l1.l_orderkey
            and o_orderstatus = 'F'
            and l1.l_receiptdate > l1.l_commitdate
            and exists (
                select * from lineitem l2
                where l2.l_orderkey = l1.l_orderkey
                    and l2.l_suppkey <> l1.l_suppkey
            )
            and not exists (
                select * from lineitem l3
                where l3.l_orderkey = l1.l_orderkey
                    and l3.l_suppkey <> l1.l_suppkey
                    and l3.l_receiptdate > l3.l_commitdate
            )
            and s_nationkey = n_nationkey
            and n_name = 'SAUDI ARABIA'
        group by s_name
        order by numwait desc, s_name
        limit 100
    """,
    22: """
        select cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal
        from (
            select substring(c_phone from 1 for 2) as cntrycode, c_acctbal
            from customer
            where
                substring(c_phone from 1 for 2) in
                    ('13','31','23','29','30','18','17')
                and c_acctbal > (
                    select avg(c_acctbal) from customer
                    where c_acctbal > 0.00
                        and substring(c_phone from 1 for 2) in
                            ('13','31','23','29','30','18','17')
                )
                and not exists (
                    select * from orders where o_custkey = c_custkey
                )
        ) as custsale
        group by cntrycode
        order by cntrycode
    """,
}


def register_tables(spark: SparkSession, data_path: str, suffix: str) -> None:
    """Read each table from S3 and register it as a temp view (once)."""
    base = data_path.rstrip("/").replace("s3://", "s3a://")
    for name in TABLES:
        path = f"{base}/{name}{suffix}"
        print(f"  registering {name} <- {path}")
        spark.read.parquet(path).createOrReplaceTempView(name)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-path",
        required=True,
        help="S3 folder holding the tables, e.g. s3://my-bucket/scale-100.0",
    )
    parser.add_argument(
        "--table-suffix",
        default=".parquet",
        help=(
            "Appended to each table name. Default '.parquet' matches the "
            "non-partitioned `make data-tables` output. Pass '' for a "
            "directory-per-table (partitioned) layout."
        ),
    )
    parser.add_argument(
        "--queries",
        default="",
        help="Comma-separated query numbers to run (default: all 1-22).",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=1,
        help="Timed iterations per query (best of N).",
    )
    parser.add_argument(
        "--warmup",
        action="store_true",
        help="Run each query once untimed before timing (hot). Default: off "
        "(cold, single-pass) to match the Trino SF400 methodology.",
    )
    parser.add_argument(
        "--timings-out",
        default="",
        help="Optional S3/local path to write a timings CSV.",
    )
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("PDS-TPCH-BENCH")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    print(f"Registering tables from {args.data_path} (suffix={args.table_suffix!r})")
    register_tables(spark, args.data_path, args.table_suffix)

    if args.queries.strip():
        query_numbers = [int(x) for x in args.queries.split(",")]
    else:
        query_numbers = sorted(QUERIES)

    # One bad query (e.g. an OOM on the heavy self-joins) must not abort the whole
    # run: record it as ERROR and keep going, and always write the CSV at the end
    # so completed timings are recoverable. Per-query lines are also printed to
    # stdout (captured in the step logs) as a second recovery trail.
    timings: list[tuple[int, str]] = []
    for q in query_numbers:
        sql = QUERIES[q]
        try:
            # Optional warm-up (JIT, metadata, caches) -- not timed. Off by
            # default so timings are cold single-pass, comparable to Trino.
            if args.warmup:
                spark.sql(sql).collect()
            best = None
            for _ in range(args.iterations):
                start = time.time()
                spark.sql(sql).collect()
                elapsed = time.time() - start
                best = elapsed if best is None else min(best, elapsed)
            assert best is not None
            print(f"q{q:<2} took: {best:.3f} s", flush=True)
            timings.append((q, f"{best:.6f}"))
        except Exception as exc:  # noqa: BLE001 - keep the run going on a bad query
            print(f"q{q:<2} FAILED: {exc}", flush=True)
            timings.append((q, "ERROR"))

    done = [(q, v) for q, v in timings if v != "ERROR"]
    total = sum(float(v) for _, v in done)
    failed = [q for q, v in timings if v == "ERROR"]
    print(f"\n{len(done)}/{len(query_numbers)} queries ok, combined: {total:.3f} s"
          + (f"; FAILED: {failed}" if failed else ""))

    if args.timings_out:
        lines = ["query_number,duration_s"]
        lines += [f"{q},{v}" for q, v in timings]
        lines.append(f"total,{total:.6f}")
        csv = "\n".join(lines) + "\n"
        # Write via Spark so it works for both s3a:// and local paths.
        sc = spark.sparkContext
        sc.parallelize([csv], 1).saveAsTextFile(args.timings_out)
        print(f"Wrote timings to {args.timings_out}")

    spark.stop()


if __name__ == "__main__":
    main()
