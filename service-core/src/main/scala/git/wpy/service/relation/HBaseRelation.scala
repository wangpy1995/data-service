package git.wpy.service.relation

import java.nio.ByteBuffer
import java.{util => ju}

import git.wpy.service.rdd.NewHBaseTableRDD
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{IdentityTableMapper, TableMapReduceUtil, TableMapper}
import org.apache.hadoop.hbase.{Cell, HBaseConfiguration, filter => hfilter}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.vectorized.ColumnarArray
import org.apache.spark.unsafe.Platform


class HBaseRelationProvider extends SchemaRelationProvider with DataSourceRegister {
  val conf = HBaseConfiguration.create()


  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val tableName = parameters("tableName")
    val startRow = Bytes.toBytes(parameters.getOrElse("startRow", ""))
    val stopRow = Bytes.toBytes(parameters.getOrElse("startRow", ""))
    HBaseRelation(sqlContext, schema, conf, tableName, startRow, stopRow)
  }

  override def shortName(): String = "hbase"
}

case class HBaseRelation(sqlContext: SQLContext,
                         schema: StructType,
                         conf: Configuration,
                         tableName: String,
                         startRow: Array[Byte],
                         stopRow: Array[Byte]
                        ) extends BaseRelation with PrunedFilteredScan {

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val scan = new Scan()
    scan.withStartRow(startRow).withStopRow(stopRow, true)

    val modelIndex = requiredColumns.indexOf(HBaseConstants.modelKey)
    val required = if (modelIndex > 0) {
      scan.addColumn(HBaseConstants.MODEL, HBaseConstants.MODEL)
      requiredColumns.drop(modelIndex)
    } else requiredColumns

    required.foreach(col => scan.addColumn(HBaseConstants.INFO, Bytes.toBytes(col)))
    val filterList = new hfilter.FilterList()
    filters.map(convertToHBaseFilter).foreach(filterList.addFilter)
    val job = Job.getInstance(conf)
    TableMapReduceUtil.initTableMapperJob(tableName, scan, classOf[IdentityTableMapper], classOf[ImmutableBytesWritable], classOf[Result], job)
    new NewHBaseTableRDD(sqlContext.sparkContext, conf).values.mapPartitions{ it =>
      val cols = requiredColumns.map(Bytes.toBytes)
      it.map { res =>
        val seq = cols.indices.map { i =>
          if (i != modelIndex) res.getValueAsByteBuffer(HBaseConstants.INFO, cols(i))
          else res.getValueAsByteBuffer(HBaseConstants.MODEL, cols(i))
        }
        Row.fromSeq(seq)
      }
    }
  }


  def convertToHBaseFilter(sparkFilter: Filter)(implicit v: (Any) => Array[Byte] = this.toBytes): hfilter.Filter = {
    sparkFilter match {
      case EqualNullSafe(qualifier, value) =>
        new hfilter.SingleColumnValueFilter(HBaseConstants.INFO, qualifier, hfilter.CompareFilter.CompareOp.EQUAL, value)
      case EqualTo(qualifier, value) =>
        new hfilter.SingleColumnValueFilter(HBaseConstants.INFO, qualifier, hfilter.CompareFilter.CompareOp.EQUAL, value)
      case GreaterThan(qualifier, value) =>
        new hfilter.SingleColumnValueFilter(HBaseConstants.INFO, qualifier, hfilter.CompareFilter.CompareOp.GREATER, value)
      case GreaterThanOrEqual(qualifier, value) =>
        new hfilter.SingleColumnValueFilter(HBaseConstants.INFO, qualifier, hfilter.CompareFilter.CompareOp.GREATER_OR_EQUAL, value)
      case LessThan(qualifier, value) =>
        new hfilter.SingleColumnValueFilter(HBaseConstants.INFO, qualifier, hfilter.CompareFilter.CompareOp.LESS, value)
      case LessThanOrEqual(qualifier, value) =>
        new hfilter.SingleColumnValueFilter(HBaseConstants.INFO, qualifier, hfilter.CompareFilter.CompareOp.LESS_OR_EQUAL, value)

      case StringStartsWith(qualifier, value) =>
        new hfilter.SingleColumnValueFilter(HBaseConstants.INFO, qualifier, hfilter.CompareFilter.CompareOp.EQUAL, value)
        new hfilter.ColumnPrefixFilter()

      case And(left, right) =>
        val filters = new ju.ArrayList[hfilter.Filter]
        filters.add(convertToHBaseFilter(left))
        filters.add(convertToHBaseFilter(right))
        new hfilter.FilterListWithAND(filters)
      case Or(left, right) =>
        val filters = new ju.ArrayList[hfilter.Filter]
        filters.add(convertToHBaseFilter(left))
        filters.add(convertToHBaseFilter(right))
        new hfilter.FilterListWithOR(filters)

      case IsNotNull(qualifier) =>
        new hfilter.SingleColumnValueFilter(HBaseConstants.INFO, qualifier, hfilter.CompareFilter.CompareOp.NOT_EQUAL, null)
      case IsNull(qualifier) =>
        new hfilter.SingleColumnValueFilter(HBaseConstants.INFO, qualifier, hfilter.CompareFilter.CompareOp.EQUAL, null)
    }
  }

  private def toBytes(value: Any): Array[Byte] = {
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
}