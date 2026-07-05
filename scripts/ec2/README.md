# SF400 single-node benchmark (Polars + DuckDB on 256 GB EC2)

Run the 22 TPC-H queries against **SF400 (~130 GB Parquet)** on one big
memory-rich box, cold single-pass, so the numbers line up with the SF400
Spark/Trino cluster runs (`benchmarks/local_spark_sf400.md`).

**Box:** `r6id.8xlarge` — 32 vCPU / **256 GiB** / 1×1900 GB local **NVMe**. The
data goes on the NVMe instance-store, not EBS, so scans hit local disk. 256 GiB is
enough that the working set fits in RAM, which is the whole point (a 96 GiB laptop
OOMs / needs streaming at this scale).

## Files

| Script | Where it runs | What it does |
|---|---|---|
| `config.env` | local + box | all knobs (instance, S3, scale, timeouts) — source it first |
| `launch.sh` | local | package repo → S3, launch instance with user-data bootstrap, print SSH + next steps. **Does not run the benchmark.** |
| `bootstrap.sh` | box (user-data) | format+mount NVMe, install python + slim deps, fetch repo |
| `stage_data.sh` | box | put SF400 parquet on NVMe as `scale-400.0/{table}.parquet` |
| `run_bench.sh` | box | cold single-pass, all 22 q, both engines, resilient per-query, upload timings |

## Run it

```bash
# 0. locally: make sure your public IP can SSH in (SG allows one IP)
source scripts/ec2/config.env
MYIP=$(curl -s https://checkip.amazonaws.com)
aws ec2 authorize-security-group-ingress --region "$AWS_REGION" \
  --group-id "$SG_ID" --protocol tcp --port 22 --cidr "$MYIP/32"   # if needed

# 1. launch (prints the instance id, DNS, and the exact next commands)
bash scripts/ec2/launch.sh

# 2. on the box (SSH in), wait for bootstrap then stage + run
cd /opt/polars-benchmark
source scripts/ec2/config.env
bash scripts/ec2/stage_data.sh      # STAGE_MODE=generate (tpchgen) by default
bash scripts/ec2/run_bench.sh       # writes output/run/timings.csv, uploads to S3

# 3. TERMINATE when done — billed per second
aws ec2 terminate-instances --region "$AWS_REGION" --instance-ids <id>
```

## Staging: generate vs copy

`STAGE_MODE` in `config.env`:

- **`generate`** (default) — `tpchgen-cli` writes the 8 tables straight to NVMe.
  No S3 egress, fast, and bit-identical to the S3 copy (that copy *was* tpchgen
  output). Best default for a fresh box.
- **`copy`** — `aws s3 cp` the shared per-table parquet from
  `s3://your_s3_bucket/scale-400.0/trino/<table>/`. Use if you specifically want
  the exact bytes the cluster runs read.

## Methodology notes

- **Cold single-pass** (`RUN_PRE_RUN=false`, one timed iteration) — matches the
  Spark/Trino SF400 runs; no warm-up flattering the second pass.
- **Per-query isolation + timeout** — each query is its own subprocess with a hard
  `QUERY_TIMEOUT`; an OOM/timeout is recorded and the run continues (the built-in
  `python -m queries.<eng>` aborts everything on the first failure).
- **Polars streaming** — `POLARS_STREAMING=true` by default (safe at SF400). With
  256 GiB you can also try `POLARS_STREAMING=false` to test the pure in-memory path
  the big box enables — the interesting single-node experiment.
- Results upload to `s3://your_s3_bucket/output/ec2_single_node/run-<stamp>/`
  (`timings.csv` + `console.log`).
