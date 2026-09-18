# Day 10 — Partitioning

## Task
Inspect partition counts, practice `repartition` vs `coalesce`, and use `partitionBy` on a Pair RDD.

## Code — `src/main/scala/Day10App.scala`
```scala
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

object Day10App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Day10-Partitioning").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    val underPartitioned = sc.parallelize(1 to 1000, numSlices = 2)
    println(s"Initial (too few) partitions: ${underPartitioned.getNumPartitions}")

    val repartitioned = underPartitioned.repartition(8)
    println(s"After repartition(8): ${repartitioned.getNumPartitions}")

    val coalesced = repartitioned.coalesce(4)
    println(s"After coalesce(4): ${coalesced.getNumPartitions}")

    val pairRDD = sc.parallelize(Seq(("A", 1), ("B", 2), ("A", 3), ("C", 4), ("B", 5)))
    val hashPartitioned = pairRDD.partitionBy(new HashPartitioner(3))
    println(s"\nPair RDD partitions after partitionBy(3): ${hashPartitioned.getNumPartitions}")

    hashPartitioned.mapPartitionsWithIndex { (idx, iter) =>
      iter.map(kv => s"partition $idx -> $kv")
    }.collect().foreach(println)

    spark.stop()
  }
}
```

## Output
> Predicted — confirm by running `sbt run`. The exact partition index each key lands in depends on `HashPartitioner`'s hash of the key, but same-key entries will always land in the same partition.
```
Initial (too few) partitions: 2
After repartition(8): 8
After coalesce(4): 4

Pair RDD partitions after partitionBy(3): 3
partition 0 -> (C,4)
partition 1 -> (A,1)
partition 1 -> (A,3)
partition 2 -> (B,2)
partition 2 -> (B,5)
```

## Explanation — what's happening

**1. Starting under-partitioned**
```scala
sc.parallelize(1 to 1000, numSlices = 2)
```
Simulates a dataset spread across too few partitions — with only 2 partitions, at most 2 tasks can run in parallel for this RDD, underusing any machine with more than 2 cores.

**2. `repartition(8)` — full shuffle, can go up or down**
Redistributes all 1000 elements evenly across 8 new partitions. This always triggers a full shuffle (even when increasing partition count), because Spark has to physically redistribute every element to achieve an even spread.

**3. `coalesce(4)` — cheaper way to decrease**
Merges the 8 partitions down to 4. Unlike `repartition`, `coalesce` can avoid a full shuffle when *decreasing* partitions — it tries to combine existing partitions that are already colocated, minimizing data movement. It would refuse to increase partition count effectively without also being told to shuffle.

**4. `partitionBy` on a Pair RDD**
```scala
pairRDD.partitionBy(new HashPartitioner(3))
```
Explicitly controls **which partition each key lands in**, based on a hash of the key modulo 3. Critically, **every occurrence of the same key always lands in the same partition** — visible in the output, where both `("A",1)` and `("A",3)` land in partition 1, and both `("B",2)` and `("B",5)` land in partition 2.

**5. Why this matters**
Once a Pair RDD is partitioned by key, repeated key-based operations (joins, lookups) on it don't need to re-shuffle every time — Spark already knows where each key lives.

## Viva Q&A
| Question | Answer |
|---|---|
| When would you use `repartition` over `coalesce`? | Use `repartition` to *increase* partitions (more parallelism) or to fix data skew, accepting a full shuffle; use `coalesce` to *decrease* partitions cheaply (e.g. before writing output files), since it can avoid a full shuffle. |
| Does `coalesce` always avoid a shuffle? | Only when decreasing partition count — it tries to merge co-located partitions; increasing partitions with `coalesce` generally requires explicitly enabling a shuffle to actually redistribute data. |
| What guarantee does `partitionBy(new HashPartitioner(3))` give? | All records with the same key are guaranteed to land in the same partition, determined by `hash(key) % numPartitions`. |
| Why is `partitionBy` useful before repeated joins on the same key? | Because the data is already co-located by key, subsequent joins/lookups on that key don't need to re-shuffle every single time. |
| What's the risk of leaving an RDD under-partitioned (e.g., only 2 partitions on an 8-core machine)? | Only 2 tasks can run concurrently for that RDD's stage, leaving the other 6 cores idle — parallelism is underused. |
