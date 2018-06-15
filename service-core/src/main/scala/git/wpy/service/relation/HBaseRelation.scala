package git.wpy.service.relation

import java.io.FileInputStream
import java.{util => ju}

import git.wpy.bean.ColumnFamily
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConverters._

class HBaseRelationProvider extends RelationProvider with DataSourceRegister {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val tableName = parameters("tableName")
    val startRow = Bytes.toBytes(parameters.getOrElse("startRow", ""))
    // ++ Array(Byte.MinValue)
    val stopRow = Bytes.toBytes(parameters.getOrElse("stopRow", ""))
    // ++ Array(Byte.MaxValue)
    val schemaFile = parameters("schemaPath")
    HBaseRelation(sqlContext, schemaFile, HBaseConstants.conf, tableName, startRow, stopRow)
  }

  override def shortName(): String = "hbase"
}

case class HBaseRelation(sqlContext: SQLContext,
                         schemaFile: String,
                         @transient conf: Configuration,
                         tableName: String,
                         startRow: Array[Byte],
                         stopRow: Array[Byte]
                        ) extends BaseRelation {
  val columnFamilies = HBaseConstants.createColumnFamilies(schemaFile)

  override def schema: StructType = StructType(Seq(StructField("rowKey", StringType)) ++ columnFamilies.map(col => StructField(col.qualifier, col.dataType)))
}

object HBaseConstants {

  val ROWKEY = Bytes.toBytes("rowKey")
  val INFO = Bytes.toBytes("info")
  val modelKey = "model"
  val MODEL = Bytes.toBytes("model")

  def createColumnFamilies(schemaFile: String) = {
    val input = new FileInputStream(schemaFile)
    val columnFamily = new Yaml().loadAs(input, classOf[ju.List[ColumnFamily]])
    val res = columnFamily.asScala.flatMap { family =>
      val familyName = Bytes.toBytes(family.getName)
      family.getQualifierList.asScala.map(f => HBaseColumn(familyName, f.getName, CatalystSqlParser.parseDataType(f.getDataType)))
    }
    input.close()
    res
  }

  val conf = HBaseConfiguration.create()
}

case class HBaseColumn(family: Array[Byte], qualifier: String, dataType: DataType) extends Serializable {
  def getNameAsBytes() = BytesHBaseColumn(family, Bytes.toBytes(qualifier), dataType)
}

case class BytesHBaseColumn(family: Array[Byte], qualifier: Array[Byte], dataType: DataType)