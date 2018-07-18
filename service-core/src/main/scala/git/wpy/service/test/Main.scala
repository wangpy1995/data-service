package git.wpy.service.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.io.StdIn

/**
  * Created by wangpengyu6 on 18-7-18. 
  */
object Main {
  val sparkConf = new SparkConf().setAppName("parquet").setMaster("local[*]")

  val ss = SparkSession.builder().config(sparkConf).getOrCreate()

  def main(args: Array[String]): Unit = {
    val df = ss.read.parquet("hdfs://10.3.67.108:8020/sec/idx/HIK_SMART_METADATA")
    df.createOrReplaceTempView("TEMP")
    //    println(ss.sql("select rowKey,device_id,face_time,face_url from TEMP where rowKey='201807935_4db2d4ff111a4e51bfcb6b404ab70586_1_477_1'").collect().mkString("\n"))
    //    println(ss.sql("select * from TEMP where rowKey='201807935_4db2d4ff111a4e51bfcb6b404ab70586_1_477_1'").queryExecution.optimizedPlan.schemaString)

    val xx = ss.sql("select * from TEMP where dt>=20180201 and dt<=20180208")
    if (args.length == 1) xx.coalesce(args(0).toInt).foreach(k => k)
    else xx.foreach(k => k)
    StdIn.readLine()
  }

}
