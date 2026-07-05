# EMR-on-EC2 cluster: architecture & where to find it

The `submit_emr_ec2.sh` launcher builds a **transient** EMR-on-EC2 cluster
(`pds-tpch-bench`), runs the 22 TPC-H queries as one `spark-submit` step, then
auto-terminates. This is what it looks like and where to see it in the console.

## Where to see it on the AWS console

1. **EMR console:** https://console.aws.amazon.com/emr — confirm the region
   (top-right) is **N. Virginia (us-east-1)**, or the cluster won't be listed.
2. Left nav → **EMR on EC2 → Clusters** → `pds-tpch-bench`.
3. Tabs on the cluster:
   - **Summary** — state, master DNS.
   - **Instances** — the primary/core/task EC2 nodes.
   - **Steps** — the `pds-tpch-benchmark` step; open it for **stdout**
     (the `q1 took: …` lines) and **stderr** (tracebacks on failure).
   - **Monitoring** — CPU/memory graphs.
   - **Application user interfaces** — link to the **Spark History Server**
     (per-query DAGs and stage timings).

The underlying machines also appear in the **EC2 console** (Instances, tagged
`aws:elasticmapreduce:...`), but manage the cluster from the EMR console.

## Architecture

```
        YOU (WSL)                         AWS  (us-east-1, vpc-xxxxxxxx)
   ┌───────────────────┐
   │ submit_emr_ec2.sh │   aws emr create-cluster
   │  (AWS CLI)        │ ─────────────────────────────┐
   └───────────────────┘   aws s3 cp job.py           │
            │ polls describe-cluster                   ▼
            │                          ┌──────────────────────────────────────┐
            │                          │        EMR cluster: pds-tpch-bench    │
            │                          │        (transient, --auto-terminate)  │
            │                          │                                       │
            │                          │  ┌─────────────────────────────────┐  │
            │                          │  │ PRIMARY (master)  m6a.xlarge     │  │
            │                          │  │  YARN ResourceManager + driver   │  │
            │                          │  │  runs your spark-submit step     │  │
            │                          │  └───────────────┬─────────────────┘  │
            │                          │                  │ schedules executors │
            │                          │  ┌───────────────▼─────────────────┐  │
            │                          │  │ CORE node(s)  m6a.4xlarge        │  │
            │                          │  │  16 vCPU / 64 GiB                 │  │
            │                          │  │  ┌────────┐┌────────┐┌────────┐  │  │
            │                          │  │  │executor││executor││executor│  │  │
            │                          │  │  │5c/16g  ││5c/16g  ││5c/16g  │  │  │
            │                          │  │  └────────┘└────────┘└────────┘  │  │
            │                          │  └──────────────────────────────────┘  │
            │                          │  ( TASK nodes: 0 now; add spot to scale)│
            │                          └───────────────┬──────────────────────┘  │
            │                                          │ reads Parquet            │
            │                                          │ (s3a://) via              │
            │                                          ▼                          │
            │                          ┌──────────────────────────────────────┐  │
            └────── results ◄──────────│  S3 GATEWAY ENDPOINT (pl-63a5400a)   │  │
                                       │  private path, rtb-ddd38ea2           │  │
                                       └───────────────┬──────────────────────┘  │
                                                       ▼
                              ┌─────────────────────────────────────────────┐
                              │  S3  s3://your_s3_bucket/                    │
                              │   ├── parq/            (8 TPC-H tables in)    │
                              │   ├── code/emr_tpch_benchmark.py  (job in)    │
                              │   ├── output/emr_timings/...   (results out)  │
                              │   └── logs/emr/...     (stdout/stderr out)    │
                              └─────────────────────────────────────────────┘
```

## The pieces

| Component | What it is | Config |
|---|---|---|
| **Primary node** | Coordinator: YARN ResourceManager + the Spark driver running your step. Doesn't crunch data. | 1× `m6a.xlarge` (small/cheap) |
| **Core node(s)** | The workers. Each runs 3 Spark executors that read S3 and execute the queries. | 1× `m6a.4xlarge` (16 vCPU / 64 GiB) |
| **Task nodes** | Optional compute-only workers (great as spot). None now. | `TASK_COUNT=0` |
| **S3 gateway endpoint** | Private, fast route from the nodes to S3 (`rtb-ddd38ea2` / `pl-63a5400a`). | free |
| **S3 bucket** | Input tables + job script; receives timings + logs. | `your_s3_bucket` |

## Mental model

The cluster is **transient** — it exists only for this run. When the step
finishes it auto-terminates and the EC2 nodes disappear (billing stops). A
`Terminated` state in the console is **success**, not an error; results and logs
persist in S3 regardless.

The single `m6a.4xlarge` core node (3 executors) does all the real work, so it's
the throughput bottleneck for the 100GB run. To go faster, raise `CORE_COUNT` or
add spot `TASK_COUNT` nodes — and bump `spark.dynamicAllocation.maxExecutors` in
`emr-spark-config.json` to `3 × (CORE_COUNT + TASK_COUNT)`.
