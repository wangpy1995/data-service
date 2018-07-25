package unsafe.mem.test

import java.io.{ByteArrayInputStream, File, FileInputStream, FileOutputStream}
import java.nio.ByteBuffer
import java.nio.channels.{ReadableByteChannel, WritableByteChannel}

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import org.apache.hadoop.io.{DataInputByteBuffer, DataOutputByteBuffer}
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.network.util.{ByteArrayReadableChannel, ByteArrayWritableChannel}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, UnsafeProjection}
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.arrow.{ArrowUtils, ArrowWriter}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnVector, ColumnarBatch}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.FunSuite

import scala.collection.JavaConverters._
import scala.io.StdIn
import scala.util.Random

class ByteBufferWritableChannel extends WritableByteChannel with ReadableByteChannel {

  val res = ByteBuffer.allocateDirect(4096)

  def rewind() = {
    res.rewind()
    this
  }

  override def write(src: ByteBuffer): Int = {
    val pos = src.position()
    while (src.remaining() > 0)
      res.put(src)
    src.position() - pos
  }

  override def isOpen: Boolean = true

  override def close(): Unit = {}

  override def read(dst: ByteBuffer): Int = {
    val pos = dst.position()
    (pos until dst.limit).foreach(_ => dst.put(res.get))
    dst.position() - pos
  }
}

class ArrowOffHeapTest extends FunSuite {

  lazy val sparkConf = new SparkConf().setMaster("local[*]").setAppName("arrow_mem")

  lazy val ss = SparkSession.builder().config(sparkConf).getOrCreate()

  import ss.implicits._

  lazy val testData = Seq((1, "aaa"), (2, "bbb"), (3, "ccc"))
  lazy val df = ss.sparkContext.parallelize(testData).toDF("id", "name")

  val file = new File("arrow_test.txt")
  if (!file.exists() || file.isDirectory) file.createNewFile()

  test("write_heap") {
    val schema = df.schema
    val timeZoneId = ss.sessionState.conf.sessionLocalTimeZone
    val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
    val allocator = ArrowUtils.rootAllocator.newChildAllocator(
      s"stdout writer for off_heap", 0, Long.MaxValue)

    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val arrowWriter = ArrowWriter.create(root)

    val rows = testData.map(kv => InternalRow(kv._1, UTF8String.fromString(kv._2)))


    val out = new FileOutputStream(file)

    val writer = new ArrowStreamWriter(root, null, out)

    writer.start()

    rows.foreach(arrowWriter.write)

    arrowWriter.finish()
    writer.writeBatch()
    arrowWriter.reset()

    writer.end()
    root.close()
    allocator.close()
  }

  test("read") {
    val rdd = ss.sparkContext.parallelize(Seq(1)).coalesce(1).mapPartitions { _ =>
      val file = new File("arrow_test.txt")
      val in = new FileInputStream(file)
      val allocator = ArrowUtils.rootAllocator.newChildAllocator(
        s"stdin reader for off_heap", 0, Long.MaxValue)
      val reader: ArrowStreamReader = new ArrowStreamReader(in, allocator)
      val root: VectorSchemaRoot = reader.getVectorSchemaRoot
      val schema = ArrowUtils.fromArrowSchema(root.getSchema)
      val columnVectors = schema.map(f => new ArrowColumnVector(root.getVector(f.name))).toArray[ColumnVector]
      val columnarBatch = new ColumnarBatch(columnVectors)
      new Iterator[Iterator[InternalRow]] {
        private var loadNextBatch = reader.loadNextBatch()

        override def hasNext: Boolean = loadNextBatch

        override def next(): Iterator[InternalRow] = {
          columnarBatch.setNumRows(root.getRowCount)
          val it = columnarBatch.rowIterator().asScala
          loadNextBatch = reader.loadNextBatch()
          it
        }
      }.flatMap(k => k)
    }
    val schema = df.schema
    val logicalPlan = LogicalRDD(
      schema.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()),
      rdd)(ss)
    val qe = ss.sessionState.executePlan(logicalPlan)
    qe.assertAnalyzed()
    new Dataset[Row](ss, logicalPlan, RowEncoder(qe.analyzed.schema)).show()
  }


  test("store_bytes") {
    def genStudent() = {
      val rowKey = UTF8String.fromString(Random.nextString(50))
      val id = Random.nextLong()
      val name = UTF8String.fromString(Random.nextString(6))
      val deviceId = UTF8String.fromString(Random.nextString(8))
      val faceTime = Random.nextLong()
      val age = Random.nextInt(100)
      val gradeValue = Random.nextInt(2)
      val plateNo = UTF8String.fromString(Random.nextString(8))
      val smile = Random.nextInt(3)
      val glass = Random.nextInt(3)
      val classifer = Random.nextInt(1024 * 1024)
      val model = new Array[Byte](534)
      Random.nextBytes(model)
      InternalRow(rowKey, id, name, deviceId, faceTime, age, gradeValue, plateNo, smile, glass, classifer, model)
    }

    val schema = StructType {
      Seq(
        StructField("rowKey", StringType),
        StructField("id", LongType),
        StructField("name", StringType),
        StructField("deviceId", StringType),
        StructField("faceTime", LongType),
        StructField("age", IntegerType),
        StructField("gradeValue", IntegerType),
        StructField("plateNo", StringType),
        StructField("smile", IntegerType),
        StructField("glass", IntegerType),
        StructField("classifier", IntegerType),
        StructField("model", BinaryType)
      )
    }

    val timeZoneId = ss.sessionState.conf.sessionLocalTimeZone

    val ttt = ss.sparkContext.parallelize(Seq(1)).coalesce(1).mapPartitions { it =>
      0 until 2000000 map (_ => genStudent()) iterator
    }.mapPartitionsWithIndex { (idx, iter) =>
      val proj = UnsafeProjection.create(schema)
      proj.initialize(idx)
      iter.map(proj)
    }.persist(StorageLevel.DISK_ONLY)
    ttt.count()

    val logicalPlan_T = LogicalRDD(
      schema.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()),
      ttt)(ss)
    val qe_T = ss.sessionState.executePlan(logicalPlan_T)
    qe_T.assertAnalyzed()
    new Dataset[Row](ss, logicalPlan_T, RowEncoder(qe_T.analyzed.schema)).show()

    StdIn.readLine()


    val data = ss.sparkContext.parallelize(Seq(1)).coalesce(1).mapPartitions { it =>
      0 until 1000000 map (_ => genStudent()) iterator
    }.mapPartitions { it =>
      val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
      val allocator = ArrowUtils.rootAllocator.newChildAllocator(
        s"stdout writer for off_heap", 0, Long.MaxValue)

      val root = VectorSchemaRoot.create(arrowSchema, allocator)
      val arrowWriter = ArrowWriter.create(root)


      val out = new DataOutputByteBuffer(1024)

      val writer = new ArrowStreamWriter(root, null, out)
      writer.start()

      def close() = {
        arrowWriter.finish()
        writer.writeBatch()
        arrowWriter.reset()

        writer.end()
        root.close()
        allocator.close()
      }

      it.foreach { row =>
        arrowWriter.write(row)
      }
      close()
      Iterator(out.getData.map(_.array()))
    }.persist(StorageLevel.DISK_ONLY)

    println(data.count())

    //read
    val row = data.mapPartitions { it =>
      val context = TaskContext.get()
      it.flatMap { bytes =>
        val in = new DataInputByteBuffer()
        in.reset(bytes.map(ByteBuffer.wrap): _*)
        val allocator = ArrowUtils.rootAllocator.newChildAllocator(
          s"stdin reader for off_heap", 0, Long.MaxValue)
        val reader: ArrowStreamReader = new ArrowStreamReader(in, allocator)
        val root: VectorSchemaRoot = reader.getVectorSchemaRoot
        context.addTaskCompletionListener { _ =>
          root.close()
          reader.close()
          allocator.close()
        }
        val schema = ArrowUtils.fromArrowSchema(root.getSchema)
        val columnVectors = schema.map(f => new ArrowColumnVector(root.getVector(f.name))).toArray[ColumnVector]
        val columnarBatch = new ColumnarBatch(columnVectors)
        new Iterator[Iterator[InternalRow]] {
          private var loadNextBatch = reader.loadNextBatch()

          override def hasNext: Boolean = loadNextBatch

          override def next(): Iterator[InternalRow] = {
            columnarBatch.setNumRows(root.getRowCount)
            val it = columnarBatch.rowIterator().asScala
            loadNextBatch = reader.loadNextBatch()
            it
          }
        }.flatMap(k => k)
      }
    }

    val logicalPlan = LogicalRDD(
      schema.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()),
      row)(ss)
    val qe = ss.sessionState.executePlan(logicalPlan)
    qe.assertAnalyzed()
    new Dataset[Row](ss, logicalPlan, RowEncoder(qe.analyzed.schema)).show()

    StdIn.readLine()
  }


}

case class Student(rowKey: String, id: Long, name: String, deviceId: String, faceTime: Long, age: Int, gradeValue: Int, plateNo: String, smile: Int, glass: Int, classifier: Int, model: Array[Byte])