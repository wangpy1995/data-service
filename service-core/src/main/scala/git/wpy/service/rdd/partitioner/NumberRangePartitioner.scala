package git.wpy.service.rdd.partitioner

import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.util.Random


class NumberRangePartitioner(ranges: Array[Long], numPartition: Int, step: Long) extends Partitioner {
  private val num = (ranges.length + 1) * numPartition
  private val min = ranges.min
  private val r = numPartition

  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[Long]
    val startOffset = ((k - min) / step) * numPartition
    val pos = startOffset.toInt + Random.nextInt(r)
    pos
  }
}

class PartialRDD[@specialized(Byte, Short, Int, Long, Float, Double) T: ClassTag](rdd: RDD[T], parts: Array[Partition], offset: Int, len: Int) extends RDD[T](rdd.sparkContext, Nil) {
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    rdd.compute(split.asInstanceOf[PartialRDDPartition].p, context)
  }

  override def getDependencies: Seq[Dependency[_]] = {
    @transient val partialPartitions = this.partitions
    Seq(new NarrowDependency[T](rdd) {
      override def getParents(partitionId: Int): Seq[Int] = {
        Seq(partialPartitions(partitionId).asInstanceOf[PartialRDDPartition].p.index)
      }
    })
  }

  override protected def getPartitions: Array[Partition] = {
    0 until len map (i => PartialRDDPartition(parts(i + offset), i)) toArray
  }
}

case class PartialRDDPartition(p: Partition, index: Int) extends Partition