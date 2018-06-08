package parquet.file.test

import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer
import scala.io.StdIn
import scala.util.Random

class BroadcastTestSuite extends FunSuite {
  private lazy val sparkConf = new SparkConf().setAppName("destroy").setMaster("local[*]")
  private lazy val sc = SparkContext.getOrCreate(sparkConf)


  test("destroy") {
    val inc = new Inc()
    val incBC = sc.broadcast(inc)

    val rdd = sc.parallelize(0 until 10).mapPartitions { p =>
      val i = incBC.value
      p.map(i.inc)
    }.cache()
    rdd.count()
    rdd.sum()
    sc.runJob(rdd,
      (context: TaskContext, it: Iterator[Int]) => {
        context.addTaskCompletionListener(_ => {
          println("destroy");
          incBC.destroy()
        })
        it.sum
      }, (a: Int, b: Int) => println(b + a)
    )
    println(rdd.sum())
    rdd.unpersist()
    println(rdd.sum())

    Console.readLine()
  }

  test("sleep") {
    val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).coalesce(4).mapPartitions { it =>
      val list = new ArrayBuffer[Int]()
      it.map { item =>
        if (item % 2 == 0)
          list += item
      }

      val time = Random.nextInt(10000)
      println(s"start sleep $time")
      Thread.sleep(time)
      println(list.mkString(", "))

      list.iterator
    }
    println(rdd.top(5))
    StdIn.readLine()
  }

  test("iterator") {
    val it = 0 until 10
    print("first: ")
    println(it.map(i => i).mkString(", "))
    print("last: ")
    println(it.map(i => i).mkString(", "))
  }

  test("union") {
    val rdd1 = sc.parallelize(Seq(1, 2, 3, 4, 5, 6))
    val rdd2 = sc.parallelize(Seq(99, 98, 97, 96, 95))

    var filteredRDD = sc.union(rdd1, rdd2).filter(_ % 2 == 0)
    println(filteredRDD.cache().count())

    filteredRDD = sc.union(rdd1, rdd2).filter(_ % 2 == 0)
    println(filteredRDD.cache().count())

    filteredRDD = sc.union(rdd1, rdd2).filter(_ % 2 == 0)
    println(filteredRDD.cache().count())

    StdIn.readLine()
  }

}

class Inc extends Serializable {
  var s = 0

  def inc(x: Int) = {
    s += x
    s
  }
}
