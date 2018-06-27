package parquet.file.test

import java.util.Calendar

import git.wpy.service.rdd.partitioner.{NumberRangePartitioner, PartialRDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

import scala.io.StdIn
import scala.util.Random

class PartitionerSuite extends FunSuite {
  private lazy val sparkConf = new SparkConf().setAppName("partition").setMaster("local[*]")
  private lazy val sc = SparkContext.getOrCreate(sparkConf)
  test("partition") {
    val calendar = Calendar.getInstance()
    val stop = calendar.getTimeInMillis

    calendar.set(2018, 4, 22)
    val start = calendar.getTimeInMillis
    val duration = stop - start
//    val data = (0 until 1000000) map (_ => (start + Random.nextDouble() * duration).toLong)

    val step = 3L * 24 * 3600 * 1000
    val arr = (0 until 13) map (start + step * _) toArray
    val numPartitions = 3
    val partitioner = new NumberRangePartitioner(arr, numPartitions, step)

    val partitionedRDD = sc.parallelize(0 until 20).repartition(20).mapPartitions { _ =>
      (0 until 500000) map (_ => (start + Random.nextDouble() * duration).toLong) map (v => v -> v) iterator
    }.partitionBy(partitioner).values
    val parts = partitionedRDD.partitions
    val rdds = arr.indices.map(i => new PartialRDD(partitionedRDD, parts, i * numPartitions, numPartitions).cache())
    val seq = rdds.map { r =>
      r.partitions
    }
    rdds.foreach { rdd => println(rdd.count()); println("=================") }

    StdIn.readLine()
  }

}
