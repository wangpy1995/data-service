package parquet.file.test

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.physical.RangePartitioning
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

import scala.io.StdIn
import scala.util.Random

class WriterTestSuite extends FunSuite {


  val sparkConf = new SparkConf().setAppName("parquet").setMaster("local[*]")

  val ss = SparkSession.builder().config(sparkConf).getOrCreate()

  val path = "/home/wangpengyu6/tmp/parquet"
  //  val path = "hdfs://10.3.67.122:8020/sec/idx/SNAP_IMAGE_INFO"

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

    val rdd = ss.sparkContext.parallelize(0 until 1000).map { i =>
      val id = i % 1000
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

    df.repartition(new Column("id")).write.partitionBy("id").mode(SaveMode.Overwrite).parquet(path)

    println(df.count())

  }


  test("read") {
    ss.read.parquet(path).createOrReplaceTempView("TEMP")
    ss.sql("select * from TEMP where face_time>=1525017600000 and face_time<1528560000000").show()
    //    val res = ss.sql("select * from TEMP where id = 9999999")show()
    //    println(res)

    //    println(res.mkString(", "))
    Console.readLine()
  }

  test("repartition") {
    println(ss.sparkContext.parallelize(Seq(1, 2, 3, 4, 5, 6)).repartition(5).repartition(3).count())
    StdIn.readLine()
  }

  test("cols") {
    val df = ss.read.parquet("hdfs://10.3.66.106:8020/sec/idx/SNAP_IMAGE_INFO")
    df.createOrReplaceTempView("TEMP")

    //    println(ss.sql("select rowKey,device_id,face_time,face_url from TEMP where rowKey='201807935_4db2d4ff111a4e51bfcb6b404ab70586_1_477_1'").collect().mkString("\n"))
    //    println(ss.sql("select * from TEMP where rowKey='201807935_4db2d4ff111a4e51bfcb6b404ab70586_1_477_1'").queryExecution.optimizedPlan.schemaString)

    ss.sql("select rowKey from TEMP where dt>=20180401 and dt<=20180501 and model IS NULL").show()
    StdIn.readLine()
    System.exit(0)
    //    df.filter(_.length > 54).show()
    //    df.filter(_.length > 55).show()
    //    df.filter(_.length > 56).show()
  }


}
