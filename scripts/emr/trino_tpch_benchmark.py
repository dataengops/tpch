"""Run the SF400 TPC-H suite against an EMR Trino coordinator and time it.

Single-pass by default: one timed run per query (no warm-up), writing a
query_number,duration_s CSV *incrementally* so a mid-run failure never loses the
queries already completed. A failed query is recorded as an error and the run
continues. Pass --warmup for the old warm-up+timed behaviour. Runs on the EMR
primary node; connects to the local coordinator on :8889.

Disclaimer: derived from the TPC-H benchmark; results are not comparable to
published TPC-H results.
"""

from __future__ import annotations

import argparse
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from trino_queries import QUERIES  # noqa: E402


def run_sql(cur, sql: str) -> None:
    """Execute and fully drain the result so timing reflects full execution."""
    cur.execute(sql)
    cur.fetchall()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=8889)
    ap.add_argument("--catalog", default="hive")
    ap.add_argument("--schema", required=True)
    ap.add_argument("--queries", default="", help="e.g. 1,6,9; empty = all 22")
    ap.add_argument("--iterations", type=int, default=1)
    ap.add_argument("--warmup", action="store_true",
                    help="run an untimed warm-up before each timed query (default: off)")
    ap.add_argument("--timings-out", default="", help="local CSV path")
    args = ap.parse_args()

    import trino

    conn = trino.dbapi.connect(
        host=args.host, port=args.port, user="hadoop",
        catalog=args.catalog, schema=args.schema,
    )
    cur = conn.cursor()

    if args.queries.strip():
        nums = [int(x) for x in args.queries.split(",")]
    else:
        nums = sorted(QUERIES)

    out_path = pathlib.Path(args.timings_out) if args.timings_out else None

    def flush(rows: list[tuple[int, str]]) -> None:
        """Rewrite the CSV after every query so a crash keeps completed rows."""
        if out_path is None:
            return
        done = [(q, v) for q, v in rows if v not in ("ERROR",)]
        total = sum(float(v) for _, v in done)
        lines = ["query_number,duration_s"]
        lines += [f"{q},{v}" for q, v in rows]
        lines.append(f"total,{total:.6f}")
        out_path.write_text("\n".join(lines) + "\n")

    timings: list[tuple[int, str]] = []
    for q in nums:
        sql = QUERIES[q]
        try:
            if args.warmup:
                run_sql(cur, sql)  # warm-up, untimed
            best = None
            for _ in range(args.iterations):
                start = time.time()
                run_sql(cur, sql)
                elapsed = time.time() - start
                best = elapsed if best is None else min(best, elapsed)
            assert best is not None
            print(f"q{q:<2} took: {best:.3f} s", flush=True)
            timings.append((q, f"{best:.6f}"))
        except Exception as exc:  # noqa: BLE001 - one bad query must not abort the run
            print(f"q{q:<2} FAILED: {exc}", flush=True)
            timings.append((q, "ERROR"))
        flush(timings)

    done = [(q, v) for q, v in timings if v != "ERROR"]
    total = sum(float(v) for _, v in done)
    failed = [q for q, v in timings if v == "ERROR"]
    print(f"\n{len(done)}/{len(nums)} queries ok, combined: {total:.3f} s"
          + (f"; FAILED: {failed}" if failed else ""))
    if out_path is not None:
        print(f"Wrote timings to {out_path}")


if __name__ == "__main__":
    main()
