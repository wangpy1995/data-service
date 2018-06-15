package git.wpy.service.test

import git.wpy.service.HBaseStrategy
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
        | startRow '201805',
        | stopRow '201807',
        | tableName 'SNAP_IMAGE_INFO'
        |)""".stripMargin)
    //"rowKey model ";
    val df = ss.sql(
      """
        |SELECT rowKey, model,  device_id, face_time, glass, sex, age_range, face_rect, ethnic, smile, classifier, human_name, birth_date, human_crednum, human_male, human_male_value, human_nation, human_nation_value, human_address, grade_value
        | FROM test_table
      """.stripMargin).persist(StorageLevel.OFF_HEAP)
    ss.sql(
      """
        |SELECT * from test_table order by rowKey limit 1000
      """.stripMargin).show(1000)
    println(df.count())
    StdIn.readLine()
  }
  val func = (a: Int) => a.formatted("%3d")
  test("serializable") {
    val funcBC = func

    println(sc.parallelize(Seq(1, 2, 3, 4, 5, 6)).map(funcBC).collect().mkString(", "))
  }

}
