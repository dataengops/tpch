# Running the SF100 benchmark locally (single machine, WSL)

The full SF100 dataset is only **~25 GB of parquet** (`lineitem` 17 GB dominates),
so a single strong box runs it comfortably — no cloud, no cluster. This is the
path for the **Ryzen 5 3600X / 96 GB** machine (6 cores / 12 threads): 96 GB RAM
holds the whole dataset in memory with room to spare.

> The Intel i7-4770K / 20 GB box is **not** worth wiring in as a second Spark
> worker — 2013 cores plus 20 GB RAM (< the 17 GB `lineitem`) make it a
> spill-heavy straggler, and a two-physical-machine WSL2 cluster needs Windows 11
> "mirrored" networking or manual port-forwarding. Use it instead as an
> independent comparison run at a smaller scale factor (e.g. `SCALE_FACTOR=10.0`).

## 0. Prerequisites (per machine)

```bash
cd <repo>                     # your WSL checkout of polars-benchmark
make .venv                    # builds .venv from requirements.txt
```

## 1. Point the repo at your local data

The query loaders resolve tables as `data/tables/scale-<SF>/<table>.parquet`
(see `settings.py` → `dataset_base_dir`). Symlink your parquet folder into that
layout so **every** engine (`run-polars`, `run-duckdb`, `run-pyspark`, …) finds it:

```bash
mkdir -p data/tables
ln -sfn "$(pwd)/datain" data/tables/scale-100.0
ls -l data/tables/scale-100.0/lineitem.parquet   # sanity check
```

(Adjust `datain` to wherever the 8 `*.parquet` files live on that machine.)

## 2a. Native engines — the repo's real purpose (recommended)

Polars and DuckDB are single-node and stream/spill gracefully; they shine on a
96 GB box where Spark struggles:

```bash
SCALE_FACTOR=100.0 make run-polars
SCALE_FACTOR=100.0 make run-duckdb
```

Keep the `.0` on the scale factor — the loader resolves `scale-100.0`.
Timings land in `output/run/` (`timings.csv` when `RUN_LOG_TIMINGS=1`).

## 2b. The same 22 SQL queries in Spark local mode

To run the **identical** queries the EMR cluster runs, reuse the EMR job in
Spark `local[*]` mode via the helper script (no cluster, no S3):

```bash
DATA_DIR=data/tables/scale-100.0 DRIVER_MEM=72g bash scripts/run_local_spark.sh
```

What it does:
- Runs `scripts/emr/emr_tpch_benchmark.py` unchanged against local parquet.
- Sets `spark.driver.memory` **before** the JVM starts (via `PYSPARK_SUBMIT_ARGS`)
  — in `local[*]` mode the executor runs inside the driver JVM, so driver memory
  is the only heap that matters. `72g` ≈ 75 % of 96 GB, leaving headroom for the
  OS + Python.
- Prints `q<N> took: <sec>` per query and writes a CSV under
  `output/local_spark_timings/`.

Env knobs: `DATA_DIR`, `DRIVER_MEM`, `CORES` (default `*` = all threads),
`SHUFFLE_PARTS` (default 24), `QUERIES` (e.g. `1,6,9`), `ITERATIONS`.

> The repo's own `make run-pyspark` also works, but its default
> `RUN_SPARK_DRIVER_MEMORY` is `2g` — far too low for SF100. Override it:
> `SCALE_FACTOR=100.0 RUN_SPARK_DRIVER_MEMORY=72g make run-pyspark`.

## 3. Tuning notes for the Ryzen (96 GB / 12 threads)

| Knob | Suggested | Why |
|---|---|---|
| `spark.driver.memory` | `72g` | Fits the 25 GB dataset + shuffle/aggregation working set; leaves ~24 GB for OS/Python. |
| `local[*]` | all 12 threads | Matches roughly the ~10 effective cores of the cloud `m6a.4xlarge` run. |
| `spark.sql.shuffle.partitions` | `24` | 200 (default) over-partitions a single node; ~2× threads is leaner. |

Expect it to land in the same ballpark as the single cloud core node — the Ryzen
is a 2019 chip vs the cloud node's 2021 silicon, but it has more RAM and zero S3
latency (data is local). For an apples-to-apples engine comparison, run
`run-polars` / `run-duckdb` alongside the Spark-local run on the same box.
