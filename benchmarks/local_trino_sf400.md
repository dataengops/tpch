# Trino SF400 TPC-H — results

**Engine:** Trino (EMR `emr-7.13.0` → Trino 479)
**Cluster:** 1 + 4× `r6id.4xlarge` = **80 vCPU / 640 GiB**, data in cluster **HDFS** (local NVMe), AWS Glue catalog
**Data:** `s3://your_s3_bucket/scale-400.0/trino/` → `s3-dist-cp` into `hdfs:///tpch/sf400/` (parquet, per-table prefixes), replicated to all 4 datanodes (`setrep`) for balanced scans
**Method:** single timed pass per query, **no warm-up** (cold), all 22 completed
**Config:** `node-scheduler.include-coordinator=true`, `query.max-memory=200GB`, `query.max-total-memory=400GB`, spill on (LZ4 → NVMe)
**Run:** cluster `j-3L7OBKDEMZ8V4` (terminated) → `s3://your_s3_bucket/output/trino_timings/run-20260704-223540/timings.csv`

## Per-query timings (seconds) — Trino vs Polars (SF400)

Polars = local Ryzen box, native Windows, Polars 1.41.2, **streaming engine**,
single node (16 vCPU / 96 GiB). Numbers from `output/run/timings.csv`
(`scale_factor=400.0`).

| Query | Trino (80 vCPU cluster) | Polars (16 vCPU local, streaming) | Faster |
|------:|------------------------:|----------------------------------:|:------|
| 1  |  67.53 |  91.48 | Trino 1.4× |
| 2  |  15.39 |   4.35 | 🐻 3.5× |
| 3  |  69.64 | 127.35 | Trino 1.8× |
| 4  |  63.59 |  33.32 | 🐻 1.9× |
| 5  |  88.94 | _did not complete_ | — |
| 6  |  61.16 |  23.93 | 🐻 2.6× |
| 7  |  78.28 | 140.13 | Trino 1.8× |
| 8  |  97.52 |  65.18 | 🐻 1.5× |
| 9  | 117.41 | 128.92 | Trino 1.1× |
| 10 |  72.13 |  58.66 | 🐻 1.2× |
| 11 |  10.97 |  11.15 | ≈ |
| 12 |  65.01 |  19.14 | 🐻 3.4× |
| 13 |  22.38 |  69.49 | Trino 3.1× |
| 14 |  61.57 |  23.50 | 🐻 2.6× |
| 15 | 113.16 |  22.10 | 🐻 5.1× |
| 16 |   5.68 |  15.83 | Trino 2.8× |
| 17 | 136.43 |  31.30 | 🐻 4.4× |
| 18 | 170.26 | 208.50 | Trino 1.2× |
| 19 |  62.16 |  34.19 | 🐻 1.8× |
| 20 |  70.85 |  54.11 | 🐻 1.3× |
| 21 | 209.82 | 489.41 | Trino 2.3× |
| 22 |   7.42 |  25.10 | Trino 3.4× |
| **total (all 22)** | **1667.3 s ≈ 27.8 min** | _n/a (q5 missing)_ | |
| **total (21, ex q5)** | **1578.4 s ≈ 26.3 min** | **1677.1 s ≈ 28.0 min** | 🐻 near-parity |

## Takeaways

- **A single 16-vCPU laptop-class box running Polars-streaming (~28.0 min, 21 q)
  essentially ties an 80-vCPU 5-node Trino cluster (~26.3 min, same 21 q).** Per
  vCPU, Polars is ~5× more efficient here — the headline result.
- **Trino wins the heavy join/aggregate tails**: q21 (triple lineitem self-join)
  209.8 s vs Polars 489.4 s (2.3×), and q3/q7/q13/q18 where its distributed
  hash-joins shine.
- **Polars wins the scan/group-heavy queries**: q15 (5.1×), q17 (4.4×), q12/q2/q22
  (~3.4×) — the streaming engine screams through single-pass aggregations.
- **q5 still does not complete on Polars-streaming** (6-table join, OOM signature —
  see `benchmarks/local_polars_120gb.md`). Trino ran it in 88.9 s.

## Cross-engine (SF400) context

| Engine | Scale | Hardware | Total (22 q unless noted) |
|---|---|---|---|
| Trino | SF400 | 5× r6id.4xlarge (80 vCPU, cluster) | 27.8 min |
| Polars (streaming) | SF400 | local Ryzen (16 vCPU, 1 node) | ~28.0 min (21 q, ex q5) |
| DuckDB | SF400 | local Ryzen (16 vCPU, 1 node) | partial (q1–q8,q18) — see timings.csv |

Method deviations vs the SF100 suite: SF400 not SF100; Trino is single-pass/cold
on a distributed cluster reading HDFS, while Polars/DuckDB are local single-node
direct parquet scans — so cross-engine numbers compare engines+hardware together,
not engines in isolation.
</content>
