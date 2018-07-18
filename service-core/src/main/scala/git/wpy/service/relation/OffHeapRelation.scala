package git.wpy.service.relation

import java.nio.ByteBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.types.StructType

/**
  * Created by wangpengyu6 on 18-7-12. 
  */

class OffHeapRelationProvider extends RelationProvider with DataSourceRegister {


  override def shortName(): String = "OFF_HEAP"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    OffHeapRelation(sqlContext, null, null)
  }
}


case class OffHeapRelation(@transient sqlContext: SQLContext, schema: StructType, @transient rdd: RDD[Array[ByteBuffer]]) extends BaseRelation
