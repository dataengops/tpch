# Spark SF400 TPC-H — results

**Engine:** Spark (EMR `emr-7.13.0`), PySpark, `--deploy-mode cluster`
**Cluster:** 1 + 5× `r6id.4xlarge` = **80 vCPU / 640 GiB**, data in cluster **HDFS** (local NVMe)
**Data:** `s3://your_s3_bucket/scale-400.0/trino/` → `s3-dist-cp` into `hdfs:///tpch/sf400/`
(parquet, per-table prefixes), replicated to all datanodes (`setrep`) for balanced scans
**Method:** single timed pass per query, **no warm-up** (cold), all 22 completed
**Config:** `emr-spark-config-hdfs-5node.json` (15 executors × 5 cores / 28g, shuffle partitions 640)
**Run:** cluster `j-9P1RI09JDNFA` → `s3://your_s3_bucket/output/emr_timings/run-20260705-010443/part-00000`

## Per-query timings (seconds) — Spark vs Trino (SF400, both cold, both 80 vCPU)

Trino column from `benchmarks/local_trino_sf400.md` (same HDFS/NVMe layout,
single-pass cold). This is a like-for-like comparison: same scale, same cluster
size class, same method.

| Query | Spark (80 vCPU, cold) | Trino (80 vCPU, cold) | Faster |
|------:|----------------------:|----------------------:|:------|
| 1  | 50.043 |  67.53 | Spark 1.3× |
| 2  | 14.894 |  15.39 | ≈ |
| 3  | 34.516 |  69.64 | Spark 2.0× |
| 4  | 27.240 |  63.59 | Spark 2.3× |
| 5  | 45.267 |  88.94 | Spark 2.0× |
| 6  | 20.833 |  61.16 | Spark 2.9× |
| 7  | 37.180 |  78.28 | Spark 2.1× |
| 8  | 48.537 |  97.52 | Spark 2.0× |
| 9  | 60.063 | 117.41 | Spark 2.0× |
| 10 | 32.783 |  72.13 | Spark 2.2× |
| 11 |  8.371 |  10.97 | Spark 1.3× |
| 12 | 27.309 |  65.01 | Spark 2.4× |
| 13 | 24.123 |  22.38 | ≈ (Trino 1.1×) |
| 14 | 23.875 |  61.57 | Spark 2.6× |
| 15 | 48.189 | 113.16 | Spark 2.3× |
| 16 |  6.282 |   5.68 | ≈ (Trino 1.1×) |
| 17 | 47.660 | 136.43 | Spark 2.9× |
| 18 | 40.494 | 170.26 | Spark 4.2× |
| 19 | 25.100 |  62.16 | Spark 2.5× |
| 20 | 29.079 |  70.85 | Spark 2.4× |
| 21 | 74.538 | 209.82 | Spark 2.8× |
| 22 |  6.417 |   7.42 | Spark 1.2× |
| **total (all 22)** | **732.79 s ≈ 12.2 min** | **1667.3 s ≈ 27.8 min** | **Spark 2.3×** |

## Takeaways

- **Spark (~12.2 min) beats Trino (~27.8 min) by ~2.3× on the same 80-vCPU cluster,
  same SF400 data in HDFS, same cold single-pass method** — a clean like-for-like win.
- **Spark wins the heavy join/aggregate tails decisively**: q18 (4.2×), q6/q17
  (2.9×), q21 triple lineitem self-join (2.8×) — its vectorized whole-stage codegen
  and codegen'd hash joins scale well on the wide `r6id` boxes.
- **Only near-ties are the tiny queries**: q2, q13, q16, q22 — sub-25 s work where
  fixed overheads dominate and the engines are within ~10%.
- q11/q16/q22 are cheap index/aggregate queries; both engines finish in single-digit
  seconds.

## Cross-engine (SF400) context

| Engine | Scale | Hardware | Method | Total (22 q unless noted) |
|---|---|---|---|---|
| **Spark** | SF400 | 5× r6id.4xlarge (80 vCPU, cluster, HDFS) | cold, single-pass | **12.2 min** |
| Trino | SF400 | 5× r6id.4xlarge (80 vCPU, cluster, HDFS) | cold, single-pass | 27.8 min |
| Polars (streaming) | SF400 | local Ryzen (16 vCPU, 1 node) | direct parquet | ~28.0 min (21 q, ex q5) |
| DuckDB | SF400 | local Ryzen (16 vCPU, 1 node) | direct parquet | partial (q1–q8,q18) |

## Appendix — Run A (superseded)

Earlier partial run on **3× r6id.4xlarge (48 vCPU)** **with warm-up** (timed value
after one untimed pass), captured live from the SQL REST API before it was stopped
at q12: q1 67.69, q2 14.07, q3 52.47, q4 43.10, q5 71.83, q6 33.48, q7 58.31,
q8 79.34, q9 96.70, q10 51.90, q11 11.63, q12 43.34 (subtotal 623.85 s). Not
comparable to the table above (fewer vCPU, warm) — kept only for provenance.
</content>
