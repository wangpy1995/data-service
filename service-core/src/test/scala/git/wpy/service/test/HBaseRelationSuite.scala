package git.wpy.service.test

import git.wpy.service.HBaseStrategy
import git.wpy.service.relation.HBaseRelation
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

import scala.io.StdIn

class HBaseRelationSuite extends FunSuite {

  lazy val sparkConf = new SparkConf().setMaster("local[*]").setAppName("relation")
  sparkConf.set("spark.storage.unrollMemoryThreshold", (1024 * 1024 * 1024L).toString)
  lazy val sc = SparkContext.getOrCreate(sparkConf)

  test("relation") {
    val ss = SparkSession.builder().config(sparkConf).getOrCreate()
    ss.experimental.extraStrategies = Seq(HBaseStrategy)
    ss.sql(
      """
        |CREATE TEMPORARY VIEW test_table
        |USING git.wpy.service.relation.HBaseRelationProvider
        |OPTIONS (
        | schemaPath '/home/wangpengyu6/IdeaProjects/data-service/service-core/src/test/resources/test_schema.yml',
        | startRow '201806',
        | stopRow '201807',
        | tableName 'SNAP_IMAGE_INFO'
        |)""".stripMargin)
    //"rowKey model ";
    /*    val df = ss.sql(
          """
            |SELECT rowKey, model,  device_id, face_time, glass, sex, age_range, face_rect, ethnic, smile, classifier, human_name, birth_date, human_crednum, human_male, human_male_value, human_nation, human_nation_value, human_address, grade_value
            | FROM test_table limit 1
          """.stripMargin).persist(StorageLevel.OFF_HEAP)*/
    ss.sql(
      """
        |SELECT  * from test_table
      """.stripMargin).show()
    ss.sql(
      """
        |SELECT  * from test_table
      """.stripMargin).show()
    ss.sql(
      """
        |SELECT  * from test_table
      """.stripMargin).show()
    /*    val conf = HBaseConfiguration.create()
        val df2 = ss.baseRelationToDataFrame(
          new HBaseRelation(
            ss.sqlContext,
            "/home/wangpengyu6/IdeaProjects/data-service/service-core/src/test/resources/test_schema.yml",
            conf,
            "SNAP_IMAGE_INFO",
            Bytes.toBytes("201805"),
            Bytes.toBytes("201806")
          )).rdd.mapPartitions(iter =>
          iter.map { it =>
            it
          }
        )

        val df3 = ss.emptyDataFrame.rdd.mapPartitions(iter =>
          iter.map { it =>
            it
          }
        )
        //    println(df.take(1))
        println(df2.take(1))
        println(df3.take(1))*/
    StdIn.readLine()
  }
  val func = (a: Int) => a.formatted("%3d")
  test("serializable") {
    val funcBC = func

    println(sc.parallelize(Seq(1, 2, 3, 4, 5, 6)).map(funcBC).collect().mkString(", "))
  }

}
