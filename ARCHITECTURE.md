# Architecture

## Pipeline shape

```
S3 / HDFS clickstream  ──read──▶  DataFrame
                                       │
                                       ▼
                            Stage: Sessionize
                            (window-lag-sum trick)
                                       │
                                       ▼
                            Stage: Aggregate.daily
                            (clicks, sessions, distinct pages)
                                       │
                                       ▼
                       UserDayPartitioner (custom)
                                       │
                                       ▼
                         ParquetWriter (partition by ts_day,
                                        snappy, low-card-first
                                        column ordering)
                                       │
                                       ▼
                                S3 / HDFS sink
                                       │
                                       ▼
                              SkewDetector (v4)
```

## Where the 65% runtime cut comes from

Three things, in roughly equal share:

1. **UserDayPartitioner** (~25% of the gain). The default Spark
   `HashPartitioner` scatters one user's events across all
   partitions. Our partitioner buckets `(userId hash, day)` so
   per-user-per-day aggregations stay local — no shuffle needed
   for the reduce.

2. **Column ordering in Parquet** (~20%). Parquet stores columns
   in declaration order; the run-length encoder loves low-card
   columns first. Putting `ts_day` (low-card, used as the
   partition column anyway) before `userId` (high-card) before
   `pageUrl` (high-card) cuts compressed size by 15-25 % on the
   canonical clickstream. Smaller files = less S3 read I/O = less
   total runtime.

3. **Day-partitioned layout** (~20%). Most queries scope to a date
   range (yesterday, last 7 days, this quarter). Partitioning by
   `ts_day` lets Spark prune entire directories from the scan
   plan. Combined with predicate pushdown, a "last 7 days" query
   touches 7 directories instead of all 90.

## Sessionization

Standard window-lag-sum trick:

```sql
SELECT *,
       userId
       || '-'
       || SUM(CASE WHEN tsMs - LAG(tsMs) OVER w > 30*60_000 THEN 1 ELSE 0 END)
            OVER w
         AS derivedSessionId
FROM   events
WINDOW w AS (PARTITION BY userId ORDER BY tsMs)
```

We keep the original cookie-level `sessionId` because analytics
teams care about both: cookie sessions tell you about anonymous
journeys, derived sessions normalize for cookie expiration.

## ParquetWriter design

Three things matter for Parquet output performance:

- **Compression codec**: snappy. zstd is 10 % smaller but ~30 %
  slower to read; for hot warehouses, snappy wins.
- **Partition column placement**: first in the schema. Spark
  pushes partition predicates down to directory pruning; placing
  the column first makes the predicate pushdown match before the
  row-group reader does any work.
- **Column ordering**: low-cardinality first, then high-cardinality.
  The dictionary encoder builds smaller dictionaries on
  low-card columns; the RLE encoder runs longer when columns are
  sorted by their values. We can't sort the data (would change
  semantics) but we can put low-card columns earlier in the schema
  so they encode better by accident.

## SkewDetector (v4)

Spark stages run as fast as their slowest task. One giant
partition stalls the whole stage. The detector buckets rows by
the candidate partition key + flags any bucket whose count is >
3× the median.

The orchestrator branches on the report:

- `hasSkew = false` → run the natural plan
- `hasSkew = true` → switch to **salting**: append a small random
  suffix to the key, group, then strip the suffix and group again.
  Two passes, but each pass shuffles a balanced load.

We don't ship the salting transform here (it's project-specific
how aggressive to be). The `SkewReport` shape is the contract.

## Why local[2] in tests

`spark.master = "local[2]"` runs the whole Spark stack in-process
with 2 worker threads. Tests that assert real DataFrame
transformations actually produce the right output, but the JVM
startup cost is ~3 seconds per test file. The shared SparkSpec
trait amortizes that across all specs.

## Why "provided" scope on Spark dependencies

Spark JARs are huge (~150 MB). Bundling them into the assembly
JAR makes the artifact slow to ship. In production, the cluster
already has Spark installed, so we mark it `provided`. The test
classpath needs Spark, so sbt's `Test` configuration loads it
locally.

## What's deliberately not here

- **Real S3 IO**. We test with local file system. Production
  injects S3 paths; the `ParquetWriter.write(df, path)` API is
  identical.
- **Streaming**. `sparkscale` is batch only. For real-time
  clickstream, see [streamflow](https://github.com/SAY-5/streamflow).
- **A query layer**. We write Parquet; the analytics team queries
  it via Athena / Trino / Spark SQL.
- **Schema evolution**. The Parquet writer assumes the input
  schema is stable. Adding a column means re-deploying the
  writer; downstream readers handle the new column via Parquet's
  schema-merge.
