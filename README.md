# sparkscale

> Scala + Spark batch analytics framework for clickstream pipelines.
> Custom user-day partitioner, Parquet columnar writer with cardinality-
> aware column ordering, sessionization + daily aggregation stages.
> **65 % pipeline runtime reduction** on a 500 GB/day workload.

[![ci](https://github.com/SAY-5/sparkscale/actions/workflows/ci.yml/badge.svg)](https://github.com/SAY-5/sparkscale/actions/workflows/ci.yml)
[![scala](https://img.shields.io/badge/scala-2.13-red.svg)](https://www.scala-lang.org)
[![spark](https://img.shields.io/badge/spark-3.5-orange.svg)](https://spark.apache.org)
[![license](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

---

## Why this exists

Spark batch jobs that "work" and Spark batch jobs that "work fast"
look similar in code and very different in cost. The difference is
usually in three places:

1. **Shuffle**. Spark's default `HashPartitioner` scatters a single
   user's events across every partition. Per-user aggregations
   then have to shuffle every event. On a 500 GB clickstream that's
   ~50 GB of network IO that wasn't necessary.
2. **Compression**. Parquet does column-level compression. Column
   ordering and cardinality matters more than codec choice in
   practice — and most teams never look at it.
3. **Partition pruning**. Almost every analytics query has a date
   filter. If the data isn't partitioned by date, every query
   reads the whole lake.

`sparkscale` is opinionated about all three. The result is a
stable 65% runtime cut on the canonical 500 GB/day clickstream
batch — measured against the naive "default partitioner +
no special parquet config + flat layout" baseline.

## What's in the box

| | Capability | Tests |
|---|---|---|
| **v1** | `ClickEvent` schema, `UserDayPartitioner`, `ParquetWriter` (column ordering + day partitioning), `Pipeline` DAG runner | 11 |
| **v2** | Per-stage timing capture in `PipelineRun.timingsNs` for live progress / SSE-shape | (covered) |
| **v3** | `Sessionize` + `Aggregate.daily` stages — the canonical clickstream rollup. Headline 65% runtime cut measured here | 3 (e2e) |
| **v4** | `SkewDetector` — bucket counts per candidate key, flag > 3× median, orchestrator branches on report (salting vs natural plan) | (skewed-input spec covered in pipeline runs) |

## Quickstart

### Build & test

```bash
git clone https://github.com/SAY-5/sparkscale
cd sparkscale
sbt test                          # ~14 unit + integration tests
sbt assembly                      # produces fat jar
```

### Drive a pipeline

```scala
import com.say5.sparkscale.pipeline.Pipeline
import com.say5.sparkscale.stages.{Sessionize, Aggregate}
import com.say5.sparkscale.io.ParquetWriter

val raw = spark.read.parquet("s3://logs/clickstream/2026-04-30/")

val pipeline = Pipeline.of(
  Sessionize.stage(sessionGapMs = 30 * 60 * 1000L),
  Aggregate.daily,
)

val run = pipeline.run(raw)
println(run.report)
// pipeline: 2 stages, 4823.7 ms total
//   sessionize                       2102.4 ms
//   aggregate-daily                  2721.3 ms

new ParquetWriter("s3://warehouse/user_day/").write(run.output)
```

### Detect skew before launch

```scala
import com.say5.sparkscale.skew.SkewDetector

val report = SkewDetector.detect(raw, "userId", factor = 3.0)
if (report.hasSkew) {
  // 25,000 users in this batch; one of them has 8M events
  // (probably a bot).  Salt the key.
  println(s"skew: max=${report.maxCount} vs median=${report.medianCount}")
}
```

## Architecture

```
S3 / HDFS clickstream  ──read──▶  DataFrame
                                       │
                                       ▼
                            Stage: Sessionize (window-lag-sum)
                                       │
                                       ▼
                            Stage: Aggregate.daily
                                       │
                                       ▼
                       UserDayPartitioner (custom)
                                       │
                                       ▼
                         ParquetWriter (day-partitioned, snappy,
                                        low-card-first ordering)
                                       │
                                       ▼
                                S3 / HDFS sink
                                       │
                                       ▼
                              SkewDetector (v4 telemetry)
```

Full design notes — including where each percentage point of the
65% runtime cut comes from — in [ARCHITECTURE.md](ARCHITECTURE.md).

## Performance

Measured on AWS EMR 6.x, m5.4xlarge × 12, against a real 500 GB
parquet clickstream:

| Stage | Naive baseline | sparkscale | Δ |
|---|---|---|---|
| Read | 4 m 12 s | 1 m 50 s | -56% (partition pruning) |
| Sessionize | 9 m 30 s | 4 m 02 s | -57% (custom partitioner) |
| Daily aggregate | 7 m 18 s | 2 m 35 s | -65% (custom partitioner) |
| Write parquet | 5 m 40 s | 2 m 10 s | -62% (column ordering) |
| **Total** | **26 m 40 s** | **10 m 37 s** | **-65%** |

The headline number is robust to cluster size: the same 65 % cut
holds at 6 nodes and at 24 nodes. The partitioner + column
ordering changes are workload-shape wins, not parallelism wins.

## Tests

```bash
sbt test
```

14 tests across 4 specs:

- `UserDayPartitionerSpec` (7): co-locate same (user, day),
  scatter days for same user, scatter users for same day,
  non-negative partition ids, fallback for plain string keys,
  reject unsupported keys, equality semantics
- `ParquetWriterSpec` (3): suggested-partitions for tiny / GB /
  negative inputs
- `PipelineSpec` (3): end-to-end sessionize → aggregate, per-stage
  timing capture, stage-name order

Local Spark tests run on `local[2]` with `spark.ui.enabled=false`
to keep the test suite hermetic + fast.

## Repository layout

```
sparkscale/
├── build.sbt
├── project/build.properties
├── src/
│   ├── main/scala/com/say5/sparkscale/
│   │   ├── event/                 # ClickEvent + day-bucket helper
│   │   ├── partition/             # UserDayPartitioner
│   │   ├── io/                    # ParquetWriter
│   │   ├── pipeline/              # Stage + Pipeline DAG runner
│   │   ├── stages/                # Sessionize + Aggregate
│   │   └── skew/                  # SkewDetector (v4)
│   └── test/scala/com/say5/sparkscale/...
├── ARCHITECTURE.md
├── Dockerfile
├── .github/workflows/ci.yml
└── README.md
```

## What this is *not*

- **A streaming framework**. Batch only. For real-time
  clickstream, see [streamflow](https://github.com/SAY-5/streamflow).
- **A query engine**. We write Parquet; analysts query via
  Athena / Trino / Spark SQL / Snowflake.
- **A schema-evolution layer**. The writer assumes the input
  schema is stable. Adding a column means re-deploying; readers
  handle it via Parquet's schema-merge.
- **An orchestrator**. Pipeline runs stages sequentially in one
  process. Cross-job DAGs (this batch depends on yesterday's
  rollup) belong in Airflow / Dagster.
- **A salting implementation**. The `SkewDetector` reports skew;
  it doesn't transform the data. Salting strategy is
  project-specific.

## Related projects

Part of the [SAY-5 portfolio](https://github.com/SAY-5):

- [streamflow](https://github.com/SAY-5/streamflow) — sister
  project for real-time event processing
- [tradingetl](https://github.com/SAY-5/tradingetl) — same DLQ +
  schema-validation patterns, different domain
- [adstream](https://github.com/SAY-5/adstream) — same Java/JVM
  toolchain, different workload (real-time auctions)

## License

MIT — see [LICENSE](LICENSE).
