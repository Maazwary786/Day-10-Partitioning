import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

object Day10App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Day10-Partitioning").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    // Start with a dataset spread across too FEW partitions (simulating the scenario)
    val underPartitioned = sc.parallelize(1 to 1000, numSlices = 2)
    println(s"Initial (too few) partitions: ${underPartitioned.getNumPartitions}")

    // repartition - full shuffle, can increase OR decrease partitions, rebalances evenly
    val repartitioned = underPartitioned.repartition(8)
    println(s"After repartition(8): ${repartitioned.getNumPartitions}")

    // coalesce - avoids a full shuffle when DECREASING partitions (merges existing ones)
    val coalesced = repartitioned.coalesce(4)
    println(s"After coalesce(4): ${coalesced.getNumPartitions}")

    // When each helps:
    // - repartition: use to INCREASE partitions (more parallelism) or to fix skew,
    //   accepting the cost of a full shuffle.
    // - coalesce: use to DECREASE partitions cheaply (e.g. before writing output
    //   files) since it can avoid shuffling by combining partitions on the same node.

    // partitionBy on a Pair RDD - controls how KEYS are distributed across partitions
    val pairRDD = sc.parallelize(Seq(("A", 1), ("B", 2), ("A", 3), ("C", 4), ("B", 5)))
    val hashPartitioned = pairRDD.partitionBy(new HashPartitioner(3))
    println(s"\nPair RDD partitions after partitionBy(3): ${hashPartitioned.getNumPartitions}")

    hashPartitioned.mapPartitionsWithIndex { (idx, iter) =>
      iter.map(kv => s"partition $idx -> $kv")
    }.collect().foreach(println)
    // Same keys always land in the same partition after partitionBy - this is
    // essential before doing repeated joins/lookups on that key, since it avoids
    // re-shuffling on every subsequent operation.

    spark.stop()
  }
}
