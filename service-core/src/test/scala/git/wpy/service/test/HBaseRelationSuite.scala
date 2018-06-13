package git.wpy.service.test

import git.wpy.service.{HBaseStrategy, HBaseTableScanExec}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

import scala.io.StdIn

class HBaseRelationSuite extends FunSuite {

  lazy val sparkConf = new SparkConf().setMaster("local[*]").setAppName("relation")
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
        | startRow '201804',
        | stopRow '201807',
        | tableName 'SNAP_IMAGE_INFO'
        |)""".stripMargin)

    ss.sql(
      """
        |SELECT count(*) FROM test_table
        |WHERE face_time >= 1525017600000 AND face_time < 1528560000000
      """.stripMargin).show()
    StdIn.readLine()
  }

}
