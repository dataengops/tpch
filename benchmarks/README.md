# Benchmark baselines

Reference runs of the 22 TPC-H (PDS) queries, for comparing new configs
(local Ryzen, scaled-up cloud, faster instance types) against a known point.

Raw per-query CSVs live beside this file (`query_number,duration_s`, matching the
`emr_tpch_benchmark.py --timings-out` format) so you can diff them directly.

## Baseline: EMR, SF100, 1× m6a.4xlarge

`baseline_emr_sf100_1x_m6a4xl.csv` — the authoritative timings the job wrote to
`s3://your_s3_bucket/output/emr_timings/run-20260701-001641/` (cross-checked
against the Spark event log; identical to the decimal).

**Setup**

| Field | Value |
|---|---|
| Date | 2026-07-01 |
| Engine | PySpark (EMR 7.5.0, Spark) |
| Cluster | 1× `m6a.xlarge` primary + 1× `m6a.4xlarge` core (transient, auto-terminate) |
| Effective workers | 2 executors × 5 cores = **10 task cores** (driver takes 4 of the 16 vCPU) |
| Data | SF100, ~25 GB parquet in `s3://your_s3_bucket/parq/` (lineitem 17 GB) |
| Spark tuning | `scripts/emr/emr-spark-config.json` (5c/16g executors, 3/node) |
| Method | 1 warm-up (untimed) + 1 timed collect per query; min reported |
| Cost | ~$1.08/hr → ~$1.50 for the run (~82 min incl. warm-up) |

**Result: 2,435.8 s (40.6 min) timed total.** No spill, no OOM, all 22 completed.

| Q | s | Q | s | Q | s | Q | s |
|---|---|---|---|---|---|---|---|
| 1 | 146.6 | 7 | 128.7 | 13 | 46.6 | 19 | 112.8 |
| 2 | 18.3 | 8 | 125.7 | 14 | 100.3 | 20 | 101.8 |
| 3 | 118.4 | 9 | 164.1 | 15 | 184.6 | **21** | **281.3** |
| 4 | 97.4 | 10 | 108.6 | 16 | 15.5 | 22 | 15.2 |
| 5 | 126.5 | 11 | 23.2 | 17 | 155.2 | | |
| 6 | 84.8 | 12 | 117.7 | 18 | 162.7 | | |

**Notes**
- **Q21 dominates** (281 s, ~12%); Q15/Q9/Q18/Q17 are the rest of the heavy tail —
  all lineitem-join-bound. Fast ones (Q22/Q16/Q2/Q11) barely touch lineitem.
- Warm-up ≈ timed (e.g. Q21 273→281) → compute/scan-bound, not IO-latency-bound.
- Bottleneck is the 10 worker cores, not memory or IO. Partitioning lineitem
  wouldn't help the tall poles (they join all of lineitem); more cores would.

## How to compare a new run

The local Spark run (`scripts/run_local_spark.sh`) and native engines
(`make run-polars` / `run-duckdb`) emit the same `query_number,duration_s` shape.
Diff a new CSV against the baseline, e.g.:

```bash
paste -d, \
  <(cut -d, -f2 benchmarks/baseline_emr_sf100_1x_m6a4xl.csv) \
  <(cut -d, -f2 output/local_spark_timings/part-00000)
```

Planned comparison points (add CSVs as they're produced):
- `local_ryzen_sf100_polars.csv` — Ryzen 5 3600X / 96 GB, Polars.
- `local_ryzen_sf100_spark.csv` — same box, Spark local mode (`run_local_spark.sh`).
- `emr_sf100_4x_m6a4xl.csv` — after the 96-vCPU quota bump, `CORE_COUNT=2 TASK_COUNT=2`.

please create benchmark file @benchmarks\comparison.md  between local spark, emr , polars and duckDB