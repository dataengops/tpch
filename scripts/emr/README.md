# Running the PDS/TPC-H benchmark on EMR-on-EC2

Runs this repo's **22 TPC-H (PDS) queries** on a **transient EMR-on-EC2 cluster**
against Parquet tables in S3. Same cluster pattern as `conv/aws/emr` (pick your
instance type, attach one `spark-submit` step, auto-terminate), but the job is
the benchmark suite instead of the MPD JSON→Parquet conversion.

Target instance: **`m6a.4xlarge`** — 16 vCPU / 64 GiB AMD EPYC. YARN exposes
~56 GiB; the clean fit is **3 executors per node** (5 cores / 16g each).

## Files

| File | Purpose |
|---|---|
| `submit_emr_ec2.sh` | One-shot: uploads the job, launches a transient cluster with the step, waits, auto-terminates. |
| `emr_tpch_benchmark.py` | Self-contained PySpark job: reads the 8 tables from S3, times all 22 queries, optionally writes a timings CSV. |
| `emr-spark-config.json` | `spark-defaults` executor sizing (`--configurations`). |

## Prerequisites

1. **Data in S3.** Generate and upload the tables first (see `../../setup_wsl.md`):
   ```bash
   SCALE_FACTOR=100.0 make data-tables
   aws s3 sync data/tables/scale-100.0/ s3://YOUR_BUCKET/scale-100.0/ \
     --exclude "*" --include "*.parquet"
   ```
   This yields `s3://YOUR_BUCKET/scale-100.0/lineitem.parquet`, etc.
2. **AWS CLI v2** configured (`aws configure`) — verify with `aws sts get-caller-identity`.
3. **IAM permissions** on that user to run EMR — see "IAM setup" below.
4. **A subnet that can reach S3** (S3 gateway endpoint or NAT).

## IAM setup (one-time)

`aws configure` only authenticates you; your IAM user also needs *permission* to
run EMR. The workflow touches four things:

| Step in the script | Permissions needed |
|---|---|
| `aws emr create-default-roles` | IAM: create the EMR roles + instance profile (`CreateRole`, `CreateInstanceProfile`, `AddRoleToInstanceProfile`, `AttachRolePolicy`, `PassRole`) — **one-time only** |
| `aws s3 cp` (upload job) | S3 write to your bucket |
| `aws emr create-cluster` | `elasticmapreduce:RunJobFlow`, EC2 provisioning, `iam:PassRole` for the EMR roles |
| `aws emr describe-cluster` | `elasticmapreduce:DescribeCluster` |

Find your user's name (`aws sts get-caller-identity` → the `Arn`), then pick one
approach. Attaching policies requires IAM admin rights (this user, or root/admin).

**Option A — simplest (personal / sandbox account):** attach admin, run, detach.

```bash
aws iam attach-user-policy --user-name yourname \
  --policy-arn arn:aws:iam::aws:policy/AdministratorAccess
# ... run the benchmark ...
aws iam detach-user-policy --user-name yourname \
  --policy-arn arn:aws:iam::aws:policy/AdministratorAccess   # optional cleanup
```

**Option B — least privilege:** attach the EMR + S3 managed policies, and create
the default roles once using an admin identity (the only step needing IAM write).

```bash
aws iam attach-user-policy --user-name yourname \
  --policy-arn arn:aws:iam::aws:policy/AmazonElasticMapReduceFullAccess
aws iam attach-user-policy --user-name yourname \
  --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess

# one-time, as an admin identity:
aws emr create-default-roles --region us-east-1
```

Once `EMR_DefaultRole` and `EMR_EC2_DefaultRole` exist, the script's
`create-default-roles` call is a no-op and the scoped user can launch clusters
without any IAM-write permission.

> Never put access keys in the repo, the scripts, or version control — the CLI
> reads them from `~/.aws/credentials` (`aws configure`).

## Quick start

```bash
export REGION=us-east-1
export BUCKET=your-bucket
export SUBNET_ID=subnet-xxxxxxxxxxxxxxxxx
export DATA_PREFIX=scale-100.0          # folder under the bucket

bash scripts/emr/submit_emr_ec2.sh
```

The script: (1) `aws emr create-default-roles`, (2) uploads
`emr_tpch_benchmark.py` to `s3://$BUCKET/code/`, (3) launches an `m6a.4xlarge`
core node + small `m6a.xlarge` primary with the benchmark step attached, (4)
polls to a terminal state and prints where the timings CSV landed.
`--auto-terminate` shuts the cluster down when the step finishes.

## Scale knobs (env vars)

| Var | Default | Meaning |
|---|---|---|
| `CORE_COUNT` | `1` | Number of `m6a.4xlarge` core nodes. |
| `TASK_COUNT` | `0` | Extra task nodes (make these spot to scale cheaply). |
| `CORE_TYPE` / `PRIMARY_TYPE` | `m6a.4xlarge` / `m6a.xlarge` | Instance types. |
| `DATA_PREFIX` | `scale-100.0` | S3 folder holding the tables. |
| `TABLE_SUFFIX` | `.parquet` | `''` for a directory-per-table (partitioned) layout. |
| `QUERIES` | *(all)* | e.g. `1,6,9` to run a subset. |
| `ITERATIONS` | `1` | Timed iterations per query after a warm-up. |

**When you change node counts, bump `spark.dynamicAllocation.maxExecutors` in
`emr-spark-config.json` to `3 × (CORE_COUNT + TASK_COUNT)`** so Spark actually
uses the extra nodes.

### Faster instance types (current-gen)

`m6a` (2021 AMD) is the cheap, safe default. To close the per-core gap with
newer hardware — e.g. the `m8id` fleet the [Polars PySpark
benchmark](https://pola.rs/posts/polars-pyspark-benchmarks/) used — set
`CORE_TYPE` (and optionally `PRIMARY_TYPE`) to a current generation:

| `CORE_TYPE` | Gen / CPU | 4xlarge = vCPU/RAM | Local NVMe | When |
|---|---|---|---|---|
| `m6a.4xlarge` *(default)* | 2021 AMD EPYC | 16 / 64 GiB | no (EBS) | cheapest; fine — this job doesn't spill |
| `m7i.4xlarge` | 2023 Intel Sapphire Rapids | 16 / 64 GiB | no | ~15–20% faster/core, still cheap |
| `m8i.4xlarge` | 2024 Intel | 16 / 64 GiB | no | newest general-purpose Intel |
| `m8id.4xlarge` | 2024 Intel + NVMe | 16 / 64 GiB | yes | matches the Polars blog fleet; NVMe helps once shuffles spill |
| `m7gd.4xlarge` | Graviton3 + NVMe | 16 / 64 GiB | yes | best price/perf **if** ARM-compatible |

All keep the `4xlarge` = 16 vCPU shape, so **the `3 × 5-core` executor packing in
`emr-spark-config.json` still fits** — no Spark retuning needed, just a swap:

```bash
export CORE_TYPE=m7i.4xlarge      # or m8id.4xlarge for NVMe
```

Notes: newer/NVMe families cost more per hour and each has its **own vCPU service
quota** (separate from Standard `m6a`) — check before launching. This job showed
zero spill at SF100, so NVMe (`*d` types) mainly matters at larger scale factors.

### Scaling out with spot task nodes

Keep CORE on-demand (shuffle stability); put scale-out on cheap spot TASK nodes.
See `conv/aws/emr/aws_emr_ec2_runbook.md` for the `--instance-fleets` block —
swap it in for `--instance-groups` in `submit_emr_ec2.sh`.

## Local smoke test (optional)

The job runs anywhere PySpark does. Against local Parquet:

```bash
.venv/bin/python scripts/emr/emr_tpch_benchmark.py \
  --data-path data/tables/scale-1.0 --queries 1,6
```

## Cost note

The cluster is transient (`--auto-terminate`) — you pay only for the run
(provisioning adds ~5–8 min). If you keep it alive for repeated runs, it bills
per second until `aws emr terminate-clusters --cluster-ids <id>`.

> Disclaimer: derived from TPC-H; results are **not** comparable to published
> TPC-H results. See the repo README.

## Trino (SF400)

Runs the 22 TPC-H queries on **Trino** on a transient EMR cluster (1× m7i.xlarge
primary + 2× r6id.8xlarge core = 68 vCPU), reading SF400 parquet from S3 through
AWS Glue. See `docs/superpowers/specs/2026-07-03-sf400-trino-emr-design.md`.

Prereqs: flat SF400 parquet at `s3://$BUCKET/scale-400.0/{table}.parquet`; a
subnet with S3 **and** internet access (pip installs on the primary); default
EMR roles with S3 + Glue permissions.

Launch:

    BUCKET=your_s3_bucket SUBNET_ID=subnet-xxxx REGION=us-east-1 \
      bash scripts/emr/submit_emr_trino.sh

The command-runner step runs `trino_run.sh` on the primary: it reshapes the flat
parquet into per-table prefixes (`scale-400.0/trino/{table}/`), derives Glue
external-table DDL from the parquet footers (`trino_setup_catalog.py`), runs the
benchmark (`trino_tpch_benchmark.py` + `trino_queries.py`), and uploads
`timings.csv` under `s3://$BUCKET/output/trino_timings/run-<stamp>/`. The cluster
auto-terminates.

Offline checks (no cloud): `.venv/Scripts/python.exe scripts/emr/tests/test_trino_queries.py`
and `.../test_trino_setup_catalog.py`.
