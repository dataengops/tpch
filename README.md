# Polars Decision Support (PDS) benchmarks

## Disclaimer
This repo is an extended copy of the existing Polars Decision Support (PDS) benchmarks  https://pola.rs/posts/polars-pyspark-benchmarks/

These benchmarks are our adaptation of an industry-standard decision support benchmark often used in the DataFrame library community. PDS consists of the same 22 queries as the industry standard benchmark TPC-H, but has modified parts for dataset generation and execution scripts.

From the [TPC website](https://www.tpc.org/tpch/):
> TPC-H is a decision support benchmark. It consists of a suite of business-oriented ad hoc queries and concurrent data modifications. The queries and the data populating the database have been chosen to have broad industry-wide relevance. This benchmark illustrates decision support systems that examine large volumes of data, execute queries with a high degree of complexity, and give answers to critical business questions.

## License

PDS is licensed under Apache License, Version 2.0.

You may not use PDS except in compliance with the Apache License, Version 2.0 and the TPC EULA.

## Generating PDS Benchmarking Data

### Project setup

```shell
# clone this repository
git clone https://github.com/dataengops/tpch.git
cd tpch/tpch-dbgen

# build tpch-dbgen
make
```

### Execute

```shell
# change directory to the root of the repository
cd ../
./run.sh
```

This will do the following,

- Create a new virtual environment with all required dependencies.
- Generate data for benchmarks.
- Run the benchmark suite.

---

# Reproducing the SF400 cross-engine benchmark

This fork adds an **SF400 (~118 GB Parquet)** cross-engine comparison of **Spark,
Trino, Polars, and DuckDB**, run cold single-pass over all 22 TPC-H queries. The
measured numbers live in `benchmarks/local_ec2_sf400_256g.md`.

**Headline result**

| Engine | Hardware | Total (22 q) |
|---|---|---:|
| **DuckDB** | 32 vCPU single node · Linux/NVMe | **3.3 min** |
| **Polars** (streaming) | 32 vCPU single node · Linux/NVMe | **5.0 min** |
| **DuckDB** | 16 vCPU single node · Linux/NVMe | **6.8 min** |
| **Spark** | 80 vCPU cluster · EMR/HDFS | 12.2 min |
| Trino | 80 vCPU cluster · EMR/HDFS | 27.8 min |

A single Linux node beats the 80-vCPU 5-node cluster. Full method, per-query
tables, and OOM behavior are in `benchmarks/local_ec2_sf400_256g.md`.

## Prerequisites (all paths)

- **AWS CLI v2** configured (`aws configure`; verify `aws sts get-caller-identity`).
- **An S3 bucket** for data + timings (below: `your_s3_bucket` — use your own).
- **Default EMR roles** (`EMR_DefaultRole`, `EMR_EC2_DefaultRole`); the EMR
  scripts create them with `aws emr create-default-roles`. See
  `scripts/emr/README.md` → *IAM setup* for the least-privilege breakdown.
- **A subnet that can reach S3** (S3 gateway endpoint or NAT).
- **An EC2 key pair** (`.pem`) for the single-node path (SSH). Keep it out of
  version control.

> Every cluster/instance is billed per second. The EMR paths auto-terminate on
> success; the EC2 path you terminate yourself. See **Cleanup** below.

## Step 0 — Generate SF400 data into S3

Generate the 8 TPC-H tables at scale factor 400 and upload them. The canonical
layout shared by all engines is **one directory per table** under
`s3://$BUCKET/scale-400.0/trino/{table}/`.

```bash
# fast native generator (Rust); one file per table
tpchgen-cli --scale-factor 400 --format=parquet --parquet-compression snappy \
  --output-dir ./sf400
# upload as per-table directories
for t in customer lineitem nation orders part partsupp region supplier; do
  aws s3 cp "./sf400/${t}.parquet" "s3://$BUCKET/scale-400.0/trino/${t}/${t}.parquet"
done
```

Smaller scales for a smoke test use the repo's own generator:
`SCALE_FACTOR=10.0 make data-tables`. The single-node path
can also **generate the data in place on the box** (no S3 egress) — that's its
default (`STAGE_MODE=generate`).

## Path A — Spark on EMR (80 vCPU, HDFS)

5× `r6id.4xlarge` core nodes (80 vCPU); data is `s3-dist-cp`'d into HDFS on local
NVMe and replicated to every datanode so scans balance across the cluster.

```bash
RELEASE=emr-7.13.0 REGION=us-east-1 BUCKET=your_s3_bucket \
SUBNET_ID=subnet-xxxx DATA_PREFIX=scale-400.0/trino \
HDFS_BASE=hdfs:///tpch/sf400 SPARK_CONFIG=emr-spark-config-hdfs-5node.json \
PRIMARY_TYPE=m7i.xlarge CORE_TYPE=r6id.4xlarge CORE_COUNT=5 \
AUTO_TERMINATE=false \
bash scripts/emr/submit_emr_ec2.sh
```

The job (`scripts/emr/emr_tpch_benchmark.py`) times all 22 queries cold and writes
`timings.csv` under `s3://$BUCKET/output/emr_timings/run-<stamp>/`. Drop
`HDFS_BASE`/`SPARK_CONFIG` (and set `CORE_TYPE=m6a.4xlarge`) to read straight from
S3 at smaller scale. Details + scale knobs in `scripts/emr/README.md`.

## Path B — Trino on EMR (80 vCPU, HDFS)

```bash
RELEASE=emr-7.13.0 REGION=us-east-1 BUCKET=your_s3_bucket \
SUBNET_ID=subnet-xxxx \
bash scripts/emr/submit_emr_trino.sh
```

The command-runner step reshapes the flat parquet into per-table prefixes, derives
Glue external-table DDL from the parquet footers (`trino_setup_catalog.py`), runs
the suite (`trino_tpch_benchmark.py` + `trino_queries.py`), and uploads
`timings.csv` under `s3://$BUCKET/output/trino_timings/run-<stamp>/`. The cluster
auto-terminates. See `scripts/emr/README.md` → *Trino (SF400)*.

## Paths C & D — Polars + DuckDB on a single EC2 node

One big memory-rich box with local NVMe. `config.env` holds every knob; swap
`INSTANCE_TYPE` between the two runs:

- **256 GiB / 32 vCPU** — `r6id.8xlarge` (both engines sweep all 22)
- **128 GiB / 16 vCPU** — `r6id.4xlarge` (one cluster node; DuckDB all 22, Polars
  streaming OOMs q5/q18)

```bash
# 0. locally: allow your public IP to SSH in (the SG allows one IP)
source scripts/ec2/config.env
MYIP=$(curl -s https://checkip.amazonaws.com)
aws ec2 authorize-security-group-ingress --region "$AWS_REGION" \
  --group-id "$SG_ID" --protocol tcp --port 22 --cidr "$MYIP/32"   # if needed

# 1. package the repo -> S3 and launch the box (prints instance id, DNS, next steps)
bash scripts/ec2/launch.sh

# 2. on the box (SSH in), wait for bootstrap then stage + run
cd /opt/polars-benchmark
source scripts/ec2/config.env
bash scripts/ec2/stage_data.sh    # generates SF400 parquet onto NVMe (default)
bash scripts/ec2/run_bench.sh     # cold single-pass, both engines, uploads timings

# 3. TERMINATE when done — billed per second
aws ec2 terminate-instances --region "$AWS_REGION" --instance-ids <id>
```

`launch.sh` bootstraps the box end-to-end via user-data (formats/mounts NVMe,
installs Python + deps, fetches the repo). Timings land under
`s3://$BUCKET/output/ec2_single_node/run-<stamp>/`. Set `POLARS_STREAMING=true`
(the default; required at SF400 — the in-memory engine OOMs) and `ENGINES` to pick
engines. Full notes: `scripts/ec2/README.md`.

## Where results land & how to compare

Every path writes a `query_number,duration_s` `timings.csv` to S3. Pull them down
and diff column-wise, e.g.:

```bash
paste -d, \
  <(cut -d, -f2 duckdb_256.csv) \
  <(cut -d, -f2 spark_sf400.csv)
```

Baselines and per-query reference tables are under `benchmarks/`.

## Cleanup

```bash
# EMR (only if launched with AUTO_TERMINATE=false, or a run hung)
aws emr list-clusters --active --region us-east-1
aws emr terminate-clusters --region us-east-1 --cluster-ids <id>

# EC2 single-node box
aws ec2 describe-instances --region us-east-1 \
  --filters Name=instance-state-name,Values=running \
  --query "Reservations[].Instances[].InstanceId" --output text
aws ec2 terminate-instances --region us-east-1 --instance-ids <id>
```
