# DuckDB — SF400 (~120 GB), partial run q1–q8

Local Ryzen box, native Windows, DuckDB 1.5.3, data as parquet on local NTFS
(SF400, 117.7 GB total; `lineitem` = 79.5 GB). Batch run was stopped by user
mid-q9; only q1–q8 completed and are reported here.

**Setup**

| Field | Value |
|---|---|
| Engine | DuckDB 1.5.3, native Windows, single-node |
| Data | SF400, 117.7 GB parquet in `datain/` (ZSTD(1)) |
| Scope | q1–q8 only (run halted mid-q9 by user request) |

| Q | s |
|---|---:|
| 1 | 269.62 |
| 2 | 26.61 |
| 3 | 340.43 |
| 4 | 103.22 |
| 5 | 377.44 |
| 6 | 148.19 |
| 7 | 592.54 |
| 8 | 521.74 |

**Subtotal (q1–q8): 2,379.8 s (39.7 min).** q9–q22 not yet run.
