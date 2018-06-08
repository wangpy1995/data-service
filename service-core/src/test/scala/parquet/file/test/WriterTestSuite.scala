package parquet.file.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SaveMode, SparkSession, types}
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

import scala.util.Random

class WriterTestSuite extends FunSuite {


  val sparkConf = new SparkConf().setAppName("parquet").setMaster("local[*]")

  val ss = SparkSession.builder().config(sparkConf).getOrCreate()

  val path = "/home/wangpengyu6/tmp/parquet"

  test("write") {
    /**
      * Schema:
      * ---------------------------------------------
      * | int | Map | List | long | string | Map |
      * ---------------------------------------------
      */

    val schema = StructType {
      Seq(
        StructField("id",
          IntegerType),
        StructField("class_grade",
          MapType(StringType, IntegerType)),
        StructField("class",
          ArrayType(StringType)),
        StructField("score", LongType),
        StructField("name", StringType),
        StructField("prefer", MapType(StringType, ArrayType(StringType)))
      )
    }

    val balls = Array("football", "baseball", "basketball", "ping-pong", "tennis", "badminton", "soccer")
    val literature = Array("animation", "movie", "magazine", "literature", "fiction", "novel")
    val food = Array("meat", "fish", "beef", "noodle", "rice", "bread", "bun", "dumpling")
    val fruit = Array("orange", "apple", "pineapple", "pear", "banana", "grape", "pitaya")

    val rdd = ss.sparkContext.parallelize(0 until 100000000).map { i =>
      val id = i
      val class_grade = Map(
        "literature" -> Random.nextInt(150),
        "math" -> Random.nextInt(150),
        "english" -> Random.nextInt(150),
        "physics" -> Random.nextInt(120),
        "chemistry" -> Random.nextInt(105),
        "biology" -> Random.nextInt(75),
        "PE" -> Random.nextInt(100)
      )
      val cls = class_grade.keys.toArray
      val score = Random.nextLong()
      val name = Random.nextString(8)
      val prefer = Map(
        "ball" -> (0 until Random.nextInt(6)).map(_ => balls(Random.nextInt(6))).toArray,
        "literature" -> (0 until Random.nextInt(5)).map(_ => literature(Random.nextInt(5))).toArray,
        "food" -> (0 until Random.nextInt(7)).map(_ => food(Random.nextInt(7))).toArray,
        "fruit" -> (0 until Random.nextInt(6)).map(_ => fruit(Random.nextInt(6))).toArray)
      (id, class_grade, cls, score, name, prefer)
    }.map(data => Row.fromTuple(data))

    val df = ss.createDataFrame(rdd, schema)

    df.coalesce(4).write.mode(SaveMode.Append).parquet(path)

    df.show()

  }


  test("read") {
    ss.read.parquet(path).createOrReplaceTempView("TEMP")

    val res = ss.sql("select * from TEMP where id = 9999999")show()

    println(res)
    //    println(res.mkString(", "))
    Console.readLine()
  }


}
