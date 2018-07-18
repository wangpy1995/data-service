package git.wpy.service

import java.nio.ByteBuffer

import git.wpy.service.relation.OffHeapRelation
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.vectorized.MutableColumnarRow
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.annotation.meta.param

/**
  * Created by wangpengyu6 on 18-7-13. 
  */
object OffHeapStrategy extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(projectList, filter, l@LogicalRelation(relation: OffHeapRelation, output, _, _)) =>
      Nil
  }
}

case class OffHeapScanExec(
                            requestedAttributes: Seq[Attribute],
                            @(transient@param) relation: OffHeapRelation,
                            filter: Seq[Expression]
                          ) extends LeafExecNode {

  lazy val rdd = relation.rdd

  lazy val converter = schema.map(_.dataType).map(genConverter)
  val len = schema.length

  def genConverter(dataType: DataType): (InternalRow, ByteBuffer, Int) => Unit = dataType match {
    case ByteType =>
      (internalRow, buff, idx) => internalRow.setByte(idx, buff.get())
    case ShortType =>
      (internalRow, buff, idx) => internalRow.setShort(idx, buff.getShort)
    case IntegerType =>
      (internalRow, buff, idx) => internalRow.setInt(idx, buff.getInt)
    case LongType =>
      (internalRow, buff, idx) => internalRow.setLong(idx, buff.getLong)
    case FloatType =>
      (internalRow, buff, idx) => internalRow.setFloat(idx, buff.getFloat)
    case DoubleType =>
      (internalRow, buff, idx) => internalRow.setDouble(idx, buff.getDouble)
    case BooleanType =>
      (internalRow, buff, idx) => internalRow.setBoolean(idx, buff.get == 1)
    case BinaryType =>
      (internalRow, buff, idx) => {
        val len = buff.getInt
        if (len == 0) internalRow.setNullAt(idx) else {
          val bytes = new Array[Byte](len)
          internalRow.update(idx, buff.get(bytes))
        }
      }
    case StringType =>
      (internalRow, buff, idx) => {
        val len = buff.getInt
        if (len == 0) internalRow.setNullAt(idx) else {
          val bytes = new Array[Byte](len)
          buff.get(bytes)
          internalRow.update(idx, UTF8String.fromBytes(bytes))
        }
      }
    case _ =>
      (internalRow, _, idx) => internalRow.update(idx, null)
  }


  override protected def doExecute(): RDD[InternalRow] = {
    rdd.mapPartitionsWithIndex { (idx, iter) =>
      val proj = UnsafeProjection.create(schema)
      proj.initialize(idx)
      val cvt = converter
      var row: InternalRow = null
      iter.map { r =>
        row = new GenericInternalRow(len)
        r.indices.foreach(i => cvt(i)(row, r(i), i))
        proj(row)
      }
    }
  }

  override def output: Seq[Attribute] = requestedAttributes
}