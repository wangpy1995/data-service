package git.wpy.service.relation

import java.io.FileInputStream
import java.nio.ByteBuffer
import java.{util => ju}

import git.wpy.bean.ColumnFamily
import git.wpy.service.rdd.NewHBaseTableRDD
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{IdentityTableMapper, TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, filter => hfilter}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.yaml.snakeyaml.Yaml

import collection.JavaConverters._

class HBaseRelationProvider extends RelationProvider with DataSourceRegister {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val tableName = parameters("tableName")
    val startRow = Bytes.toBytes(parameters.getOrElse("startRow", ""))
    // ++ Array(Byte.MinValue)
    val stopRow = Bytes.toBytes(parameters.getOrElse("stopRow", ""))
    // ++ Array(Byte.MaxValue)
    val schemaFile = parameters("schemaPath")
    val columnFamilies = HBaseConstants.createColumnFamilies(schemaFile)
    HBaseRelation(sqlContext, columnFamilies, HBaseConstants.conf, tableName, startRow, stopRow)
  }

  override def shortName(): String = "hbase"
}

case class HBaseRelation(sqlContext: SQLContext,
                         columnFamilies: Seq[HBaseColumn],
                         conf: Configuration,
                         tableName: String,
                         startRow: Array[Byte],
                         stopRow: Array[Byte]
                        ) extends BaseRelation with PrunedFilteredScan {

  override def schema: StructType = StructType(columnFamilies.map(col => StructField(col.qualifier, col.dataType)))

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val requiredColumnSet = requiredColumns.toSet
    val requiredCols = columnFamilies.filter(family => requiredColumnSet.contains(family.qualifier))
      .map(col => (col, getAs(col.dataType)))
    val bytesCols = requiredCols.map(col => (col._1.getNameAsBytes(), col._2))

    val scan = new Scan()
    scan.withStartRow(startRow).withStopRow(stopRow, true)
    bytesCols.foreach(c => scan.addColumn(c._1.family, c._1.qualifier))

    val filterList = new hfilter.FilterList()
    filters.foreach(convertToHBaseFilter(_, filterList))
    if (!filterList.getFilters.isEmpty) {
      scan.setFilter(filterList)
    }

    scan.setCaching(1000)
    scan.setCacheBlocks(false)

    val job = Job.getInstance(conf)
    TableMapReduceUtil.initTableMapperJob(tableName, scan, classOf[IdentityTableMapper], classOf[ImmutableBytesWritable], classOf[Result], job)
    new NewHBaseTableRDD(sqlContext.sparkContext, job.getConfiguration).values.mapPartitions { it =>
      it.map { res =>
        val seq = bytesCols.map(col => col._2(res.getValueAsByteBuffer(col._1.family, col._1.qualifier)))
        Row.fromSeq(seq)
      }
    }
  }

  def getAs(dataType: DataType): ByteBuffer => Any = dataType match {
    case _: ByteType => (byteBuffer: ByteBuffer) => byteBuffer.get()
    case _: CharType => (byteBuffer: ByteBuffer) => byteBuffer.getChar()
    case _: ShortType => (byteBuffer: ByteBuffer) => byteBuffer.getShort()
    case _: IntegerType => (byteBuffer: ByteBuffer) => byteBuffer.getInt()
    case _: LongType => (byteBuffer: ByteBuffer) => byteBuffer.getLong()
    case _: FloatType => (byteBuffer: ByteBuffer) => byteBuffer.getFloat()
    case _: DoubleType => (byteBuffer: ByteBuffer) => byteBuffer.getDouble()
    case _: BinaryType => (byteBuffer: ByteBuffer) => byteBuffer.array()
    case _: StringType => (byteBuffer: ByteBuffer) => Bytes.toString(byteBuffer.array())
    case _: BooleanType => (byteBuffer: ByteBuffer) => Bytes.toBoolean(byteBuffer.array())
  }

  def convertToHBaseFilter(sparkFilter: Filter, filterList: hfilter.FilterList)(implicit v: (Any) => Array[Byte] = this.toBytes): Unit = {
    sparkFilter match {
      case EqualNullSafe(qualifier, value) =>
        filterList.addFilter(new hfilter.SingleColumnValueFilter(HBaseConstants.INFO, qualifier, hfilter.CompareFilter.CompareOp.EQUAL, value))
      case EqualTo(qualifier, value) =>
        filterList.addFilter(new hfilter.SingleColumnValueFilter(HBaseConstants.INFO, qualifier, hfilter.CompareFilter.CompareOp.EQUAL, value))
      case GreaterThan(qualifier, value) =>
        filterList.addFilter(new hfilter.SingleColumnValueFilter(HBaseConstants.INFO, qualifier, hfilter.CompareFilter.CompareOp.GREATER, value))
      case GreaterThanOrEqual(qualifier, value) =>
        filterList.addFilter(new hfilter.SingleColumnValueFilter(HBaseConstants.INFO, qualifier, hfilter.CompareFilter.CompareOp.GREATER_OR_EQUAL, value))
      case LessThan(qualifier, value) =>
        filterList.addFilter(new hfilter.SingleColumnValueFilter(HBaseConstants.INFO, qualifier, hfilter.CompareFilter.CompareOp.LESS, value))
      case LessThanOrEqual(qualifier, value) =>
        filterList.addFilter(new hfilter.SingleColumnValueFilter(HBaseConstants.INFO, qualifier, hfilter.CompareFilter.CompareOp.LESS_OR_EQUAL, value))

      case StringStartsWith(qualifier, value) =>
        filterList.addFilter(new hfilter.ColumnPrefixFilter(value))

      case And(left, right) =>
        convertToHBaseFilter(left, filterList)
        convertToHBaseFilter(right, filterList)
      case Or(left, right) =>
        convertToHBaseFilter(left, filterList)
        convertToHBaseFilter(right, filterList)

      case IsNotNull(qualifier) =>
        filterList.addFilter(new hfilter.SingleColumnValueFilter(HBaseConstants.INFO, qualifier, hfilter.CompareFilter.CompareOp.NOT_EQUAL, Array.emptyByteArray))
      case IsNull(qualifier) =>
        filterList.addFilter(new hfilter.SingleColumnValueFilter(HBaseConstants.INFO, qualifier, hfilter.CompareFilter.CompareOp.EQUAL, Array.emptyByteArray))
    }
  }

  private def toBytes(value: Any): Array[Byte] = value match {
    //number
    case bool: Boolean => Bytes.toBytes(bool)
    case byte: Byte => Array(byte)
    case short: Short => Bytes.toBytes(short)
    case int: Int => Bytes.toBytes(int)
    case long: Long => Bytes.toBytes(long)
    case float: Float => Bytes.toBytes(float)
    case double: Double => Bytes.toBytes(double)
    //string
    case char: Char => Bytes.toBytes(char)
    case string: String => Bytes.toBytes(string)
    //rich type
    case decimal: BigDecimal => Bytes.toBytes(decimal.bigDecimal)
    case binary: Array[Byte] => binary
    case null => null
    case _ => throw new UnsupportedOperationException("convert to bytes error, unsupported type.")
  }
}

object HBaseConstants {

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