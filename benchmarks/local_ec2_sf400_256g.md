# SF400 single-node — Polars + DuckDB on big-memory EC2 boxes

Two single-node runs of the SF400 TPC-H benchmark, to compare against the 80-vCPU
5-node Spark/Trino clusters:

- **256 GiB:** `r6id.8xlarge` — 32 vCPU / 247 GiB / 1×1.7 TB NVMe
- **128 GiB:** `r6id.4xlarge` — 16 vCPU / 128 GiB / 1×950 GB NVMe (**== one node of
  the SF400 Spark/Trino cluster**)

Both Ubuntu 24.04. SF400 parquet generated in place with `tpchgen-cli` onto local
NVMe (158 GB), page-cached during the run. Cold single-pass, all 22 queries,
per-query subprocess with a 1800 s timeout (resilient: an OOM/timeout is recorded
and the run continues). Date: 2026-07-05. Methodology matches the cluster runs.

## Headline

| Engine / mode | 256 GiB / 32 vCPU | 128 GiB / 16 vCPU | Queries |
|---|---|---|---|
| **DuckDB 1.5.3** | **199 s (3.3 min)** | **411 s (6.8 min)** | 22/22 both |
| **Polars 1.41.2 streaming** | **303 s (5.0 min)** | 531 s (8.8 min) | 22/22 · **20/22** |
| Polars 1.41.2 in-memory | 660 s / 20 q (q01,q09 OOM) | *not run* | — |
| *Spark cluster — 5× r6id.4xlarge / 80 vCPU / HDFS* | *12.2 min* | | 22/22 |
| *Trino cluster — same* | *27.8 min* | | 22/22 |

**A single node beats the 80-vCPU 5-node cluster — even a small one.** DuckDB on
*one* `r6id.4xlarge` (16 vCPU, the cluster's own per-node hardware) finishes all 22
in **6.8 min — ~1.8× faster than the 5-node, 80-vCPU Spark cluster (12.2 min)**. On
the 256 GiB box DuckDB (3.3 min) is ~3.7× faster than Spark. At SF400 the dataset
fits one big box's NVMe/RAM, and columnar engines skip all distributed overhead.

## Where the single-node story cracks: memory, not CPU

- **DuckDB is the robust engine.** It completes all 22 at *both* sizes; halving RAM
  and cores (256→128) merely ~2× the time (199→411 s) — clean CPU/linear scaling,
  never OOMs. Its spill-to-disk streaming keeps it alive at 128 GiB.
- **Polars streaming completes all 22 at 256 GiB but OOMs 2 queries at 128 GiB**
  (`q05`, `q18`, `rc=137`). The streaming engine still materializes some join
  intermediates that exceed 128 GiB. So 128 GiB is below Polars' comfortable SF400
  floor; 256 GiB is enough.
- **Polars in-memory (`POLARS_STREAMING=false`) is a dead end at SF400** — even at
  256 GiB it OOMs on q01/q09 (q09 wanted ~257 GB) and is 2–7× slower where it
  survives. Not re-run at 128 GiB (would only OOM more).

Bottom line: **use DuckDB, or Polars *streaming* on ≥256 GiB.** A single well-sized
box is faster and far cheaper than the distributed cluster for SF400.

## Per-query (seconds)

| q | DuckDB 256 | DuckDB 128 | Polars-stream 256 | Polars-stream 128 |
|---|---|---|---|---|
| 1  | 9.6  | 20.1 | 19.6 | 35.7 |
| 2  | 2.4  | 4.2  | 0.9  | 2.6 |
| 3  | 8.4  | 15.0 | 16.1 | 36.7 |
| 4  | 7.3  | 11.6 | 10.6 | 24.4 |
| 5  | 9.1  | 16.7 | 29.9 | **OOM** |
| 6  | 4.9  | 7.7  | 8.0  | 21.0 |
| 7  | 10.6 | 18.4 | 16.8 | 38.2 |
| 8  | 11.3 | 21.4 | 13.1 | 22.3 |
| 9  | 21.7 | 65.9 | 28.4 | 48.1 |
| 10 | 9.5  | 21.2 | 13.4 | 23.3 |
| 11 | 2.1  | 5.3  | 2.9  | 5.1 |
| 12 | 7.5  | 14.4 | 6.2  | 13.3 |
| 13 | 11.1 | 21.0 | 16.8 | 31.9 |
| 14 | 6.6  | 11.0 | 4.6  | 8.1 |
| 15 | 6.5  | 10.5 | 4.3  | 7.6 |
| 16 | 2.3  | 3.8  | 4.0  | 6.8 |
| 17 | 10.0 | 16.6 | 7.6  | 11.3 |
| 18 | 15.1 | 26.3 | 18.0 | **OOM** |
| 19 | 8.7  | 15.2 | 6.8  | 17.6 |
| 20 | 8.8  | 15.9 | 11.3 | 26.5 |
| 21 | 22.6 | 37.9 | 57.1 | 109.8 |
| 22 | 3.1  | 5.6  | 6.3  | 12.1 |
| **Σ (completed)** | **~199** | **411** | **~303** | 531 (20 q) |

## Repro

Scripts under `scripts/ec2/` (config.env, launch.sh, bootstrap.sh, stage_data.sh,
run_bench.sh, driver*.sh). Swap `INSTANCE_TYPE` in config.env between runs. Raw
timings + console logs in S3:

- 256 GiB streaming: `s3://your_s3_bucket/output/ec2_single_node/run-20260705-170752/`
- 256 GiB in-memory: `…/run-20260705-171722/`
- 128 GiB streaming: `…/run-20260705-181023/`

Launch gotchas (all fixed in commit `b727c34`): Windows `file://` user-data path
needed `cygpath`; Ubuntu 24.04 dropped the `awscli` apt package (use AWS CLI v2
bundle); Windows CRLF in tracked shell scripts broke bash (strip CR on the box).
With those in, the 128 GiB launch bootstrapped end-to-end with zero manual steps.
