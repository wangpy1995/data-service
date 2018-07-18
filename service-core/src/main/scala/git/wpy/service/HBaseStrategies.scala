package git.wpy.service

import git.wpy.service.rdd.NewHBaseTableRDD
import git.wpy.service.relation.{HBaseColumn, HBaseRelation}
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{IdentityTableMapper, TableInputFormatBase, TableMapReduceUtil}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeMap, AttributeReference, AttributeSet, Cast, Contains, EndsWith, EqualTo, Expression, GenericInternalRow, GreaterThan, GreaterThanOrEqual, InSet, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, NamedExpression, Or, StartsWith, UnsafeProjection}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{LeafExecNode, ProjectExec, SparkPlan}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.annotation.meta.param

object HBaseStrategy extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(projectList, filter, l@LogicalRelation(relation: HBaseRelation, output, _, _)) =>
      filterProject4HBase(relation, output, projectList, filter) :: Nil

    case _ => Nil
  }

  private def filterProject4HBase(relation: HBaseRelation, output: Seq[AttributeReference], projectList: Seq[NamedExpression], filterPredicates: Seq[Expression]): SparkPlan = {
    val attributeMap: AttributeMap[AttributeReference] = AttributeMap(output.map(o => (o, o)))
    val projectSet = AttributeSet(projectList.flatMap(_.references))

    val filterSet = AttributeSet(filterPredicates.flatMap(_.references))
    val filters = if (filterPredicates.nonEmpty) {
      Seq(
        filterPredicates.map {
          _ transform { case a: AttributeReference => attributeMap(a) }
        }.reduceLeft(And)
      )
    } else filterPredicates

    if (projectList.map(_.toAttribute) == projectList && projectSet.size == projectList.size && filterSet.subsetOf(projectSet)) {
      val requestedColumns = projectList.asInstanceOf[Seq[Attribute]].map(attributeMap)
      HBaseTableScanExec(requestedColumns, relation, filters)
    } else {
      //val requestedColumns = projectSet.map(relation.attributeMap ).toSeq
      val requestedColumns = attributeMap.keySet.toSeq
      val scan = HBaseTableScanExec(requestedColumns, relation, filters)
      ProjectExec(projectList, scan)
    }
  }
}

case class HBaseTableScanExec(
                               requestedAttributes: Seq[Attribute],
                               @(transient@param) relation: HBaseRelation,
                               filter: Seq[Expression])
  extends LeafExecNode {
  private val tableName = relation.tableName
  private val startRow = relation.startRow
  private val stopRow = relation.stopRow
  @transient private val hConf = relation.conf

  @transient private val columnFamilies = relation.columnFamilies

  @transient private val requiredColumns = schema.map { field =>
    if (field.name == "rowKey") HBaseColumn(null, "rowKey", StringType)
    else columnFamilies.find(_.qualifier == field.name).get
  }

  private val cols = requiredColumns.map { field =>
    (field.family, Bytes.toBytes(field.qualifier), genHBaseFieldConverter(field.dataType))
  }

  override def output: Seq[Attribute] = requestedAttributes

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def producedAttributes: AttributeSet = outputSet ++
    AttributeSet(filter.flatMap(_.references))

  override protected def doExecute(): RDD[InternalRow] = {

    val scan = new Scan().withStartRow(startRow).withStopRow(stopRow, true)
    val numOutputRows = longMetric("numOutputRows")
    val hbaseFilter = buildHBaseFilterList4Where(filter.headOption)
    addColumnFamiliesToScan(scan, hbaseFilter, filter.headOption, requestedAttributes)

    if (schema.isEmpty || (schema.length == 1 && schema.head.name == "rowKey"))
      scan.setFilter(new FirstKeyOnlyFilter())

    val job = Job.getInstance(hConf)
    TableMapReduceUtil.initTableMapperJob(tableName, scan, classOf[IdentityTableMapper], classOf[ImmutableBytesWritable], classOf[Result], job)
    job.getConfiguration.setBoolean(TableInputFormatBase.MAPREDUCE_INPUT_AUTOBALANCE, true)
    job.getConfiguration.setLong(TableInputFormatBase.MAX_AVERAGE_REGION_SIZE, 1L * 107374180)
    //    job.getConfiguration.setInt(TableInputFormatBase.NUM_MAPPERS_PER_REGION,4)
    new NewHBaseTableRDD(sqlContext.sparkContext, job.getConfiguration).mapPartitionsWithIndex { (index, iter) =>
      val proj = UnsafeProjection.create(schema)
      proj.initialize(index)
      val size = schema.length
      iter.map { result =>
        val internalRow = new GenericInternalRow(size)
        hbase2SparkRow(result._2, internalRow, cols)
        numOutputRows += 1
        proj(internalRow)
      }
    }
  }

  type CF_QUALIFIER_CONVERTER = (Array[Byte], Array[Byte], (InternalRow, Int, Array[Byte]) => Unit)

  def genHBaseFieldConverter(dataType: DataType): (InternalRow, Int, Array[Byte]) => Unit = dataType match {
    case ByteType =>
      (internalRow, i, v) => internalRow.setByte(i, v.head)

    case StringType =>
      (internalRow, i, v) => internalRow.update(i, UTF8String.fromBytes(v))

    //convert to milli seconds
    case TimestampType =>
      (internalRow, i, v) => internalRow.setLong(i, Bytes.toLong(v) * 1000)
    case LongType =>
      (internalRow, i, v) => internalRow.setLong(i, Bytes.toLong(v))
    case IntegerType =>
      (internalRow, i, v) => internalRow.setInt(i, Bytes.toInt(v))
    case ShortType =>
      (internalRow, i, v) => internalRow.setShort(i, Bytes.toShort(v))

    case BooleanType =>
      (internalRow, i, v) => internalRow.setBoolean(i, Bytes.toBoolean(v))

    case DoubleType =>
      (internalRow, i, v) => internalRow.setDouble(i, Bytes.toDouble(v))
    case FloatType =>
      (internalRow, i, v) => internalRow.setFloat(i, Bytes.toFloat(v))

    case _ =>
      (internalRow, i, v) => internalRow.update(i, v)
  }

  def hbase2SparkRow(result: Result, internalRow: InternalRow, cols: Seq[CF_QUALIFIER_CONVERTER]): Unit = {
    var i = 0
    cols.foreach { case (family, qualifier, convert) =>
      val v = if (family == null) result.getRow
      else result.getValue(family, qualifier)
      if (v == null) internalRow.setNullAt(i)
      else convert(internalRow, i, v)
      i += 1
    }
  }

  //columnFamily_QualifierName <=== requestAttribute
  def addColumnFamiliesToScan(scan: Scan, filters: Option[Filter], predicate: Option[Expression], projectionList: Seq[NamedExpression]): Scan = {
    cols.foreach(cf => if (cf._1 != null) scan.addColumn(cf._1, cf._2))
    scan.setCaching(1000)
    scan.setCacheBlocks(false)
    if (filters.isDefined) {
      scan.setFilter(filters.get)
    }
    scan
  }


  private def add2FilterList(filters: java.util.ArrayList[Filter], filtersToBeAdded: Option[FilterList], operator: FilterList.Operator) = filtersToBeAdded match {
    case Some(filterList) =>
      import collection.JavaConverters._
      val size = filterList.getFilters.size
      if (size == 1 || filterList.getOperator == operator) {
        filterList.getFilters.asScala.map(filters.add)
      } else filters.add(filterList)
    case None => {}
  }

  def createNullFilter(left: AttributeReference): Option[FilterList] = {
    requiredColumns.find(_.qualifier == left.name) match {
      case Some(cf) =>
        val filter = new SingleColumnValueFilter(cf.family, Bytes.toBytes(cf.qualifier), CompareOp.EQUAL, new NullComparator())
        filter.setFilterIfMissing(true)
        Some(new FilterList(filter))
      case None => None
    }
  }

  def createNotNullFilter(left: AttributeReference): Option[FilterList] = {
    requiredColumns.find(_.qualifier == left.name) match {
      case Some(cf) =>
        val filter = new SingleColumnValueFilter(cf.family, Bytes.toBytes(cf.qualifier), CompareOp.NOT_EQUAL, new NullComparator())
        filter.setFilterIfMissing(true)
        Some(new FilterList(filter))
      case None => None
    }
  }

  private def getBinaryValue(literal: Literal): Array[Byte] = literal.dataType match {
    case BooleanType => Bytes.toBytes(literal.value.asInstanceOf[Boolean])
    case ByteType => Bytes.toBytes(literal.value.asInstanceOf[Byte])
    case ShortType => Bytes.toBytes(literal.value.asInstanceOf[Short])
    case IntegerType => Bytes.toBytes(literal.value.asInstanceOf[Int])
    case LongType => Bytes.toBytes(literal.value.asInstanceOf[Long])
    case FloatType => Bytes.toBytes(literal.value.asInstanceOf[Float])
    case DoubleType => Bytes.toBytes(literal.value.asInstanceOf[Double])
    case StringType => UTF8String.fromString(literal.value.toString).getBytes
  }


  def createSingleColumnValueFilter(left: AttributeReference, right: Literal, compareOp: CompareOp, comparable: ByteArrayComparable = null): Option[FilterList] = {
    val nonKeyColumn = requiredColumns.find(_.qualifier == left.name)

    if (nonKeyColumn.isDefined) {
      val cf = nonKeyColumn.get
      val nullComparable = comparable == null

      var filter = new SingleColumnValueFilter(cf.family, Bytes.toBytes(cf.qualifier), compareOp, new BinaryComparator(getBinaryValue(right)))
      val filter1 = new SingleColumnValueFilter(cf.family, Bytes.toBytes(cf.qualifier), compareOp, comparable)

      if (!nullComparable) filter = filter1

      filter.setFilterIfMissing(true)
      Some(new FilterList(filter))
    }
    else {
      None
    }
  }

  def buildHBaseFilterList4Where(filter: Option[Expression]): Option[FilterList] = {
    if (filter.isEmpty) {
      None
    }
    else {
      val expression = filter.get
      expression match {
        case And(left, right) =>
          val filters = new java.util.ArrayList[Filter]

          if (left != null) {
            val leftFilterList = buildHBaseFilterList4Where(Some(left))
            add2FilterList(filters, leftFilterList, FilterList.Operator.MUST_PASS_ALL)
          }
          if (right != null) {
            val rightFilterList = buildHBaseFilterList4Where(Some(right))
            add2FilterList(filters, rightFilterList, FilterList.Operator.MUST_PASS_ALL)
          }
          Some(new FilterList(FilterList.Operator.MUST_PASS_ALL, filters))

        case Or(left, right) =>
          val filters = new java.util.ArrayList[Filter]
          if (left != null) {
            val leftFilterList = buildHBaseFilterList4Where(Some(left))
            add2FilterList(filters, leftFilterList, FilterList.Operator.MUST_PASS_ONE)
          }
          if (right != null) {
            val rightFilterList = buildHBaseFilterList4Where(Some(right))
            add2FilterList(filters, rightFilterList, FilterList.Operator.MUST_PASS_ONE)
          }
          Some(new FilterList(FilterList.Operator.MUST_PASS_ONE, filters))

        case InSet(value@AttributeReference(name, dataType, _, _), hset) =>
          val column = requiredColumns.find(_.qualifier == name)
          if (column.isDefined) {
            val cf = column.get
            val filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE)
            for (item <- hset) {
              val filter = new SingleColumnValueFilter(
                cf.family, Bytes.toBytes(cf.qualifier), CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes(item.asInstanceOf[String])))
              filterList.addFilter(filter)
            }
            Some(filterList)
          }
          else None

        case IsNull(left: AttributeReference) => createNullFilter(left)
        case IsNotNull(left: AttributeReference) => createNotNullFilter(left)
        case GreaterThan(left: AttributeReference, right: Literal) =>
          createSingleColumnValueFilter(left, right, CompareOp.GREATER)
        case GreaterThan(left: Literal, right: AttributeReference) =>
          createSingleColumnValueFilter(right, left, CompareOp.GREATER)
        case GreaterThanOrEqual(left: AttributeReference, right: Literal) =>
          createSingleColumnValueFilter(left, right, CompareOp.GREATER_OR_EQUAL)
        case GreaterThanOrEqual(left: Literal, right: AttributeReference) =>
          createSingleColumnValueFilter(right, left, CompareOp.GREATER_OR_EQUAL)
        case EqualTo(left: AttributeReference, right: Literal) =>
          createSingleColumnValueFilter(left, right, CompareOp.EQUAL)
        case EqualTo(left: Literal, right: AttributeReference) =>
          createSingleColumnValueFilter(right, left, CompareOp.EQUAL)
        case EqualTo(left: Cast, right: Literal) =>
          val leftValue: AttributeReference = left.child.asInstanceOf[AttributeReference]
          val rightDecimal = BigDecimal(right.value.toString).bigDecimal
          val rightValue: Literal = Literal(rightDecimal.stripTrailingZeros().toPlainString)
          createSingleColumnValueFilter(leftValue, rightValue, CompareOp.EQUAL)
        case LessThan(left: AttributeReference, right: Literal) =>
          createSingleColumnValueFilter(left, right, CompareOp.LESS)
        case LessThan(left: Literal, right: AttributeReference) =>
          createSingleColumnValueFilter(right, left, CompareOp.LESS)
        case LessThanOrEqual(left: AttributeReference, right: Literal) =>
          createSingleColumnValueFilter(left, right, CompareOp.LESS_OR_EQUAL)
        case LessThanOrEqual(left: Literal, right: AttributeReference) =>
          createSingleColumnValueFilter(right, left, CompareOp.LESS_OR_EQUAL)
        case StartsWith(left: AttributeReference, right: Literal) =>
          val regexStringComparator = new RegexStringComparator(".*" + right.value + "$")
          createSingleColumnValueFilter(left, right, CompareOp.EQUAL, regexStringComparator)
        case EndsWith(left: AttributeReference, right: Literal) =>
          val binaryPrefixComparator = new BinaryPrefixComparator(Bytes.toBytes(right.value.toString))
          createSingleColumnValueFilter(left, right, CompareOp.EQUAL, binaryPrefixComparator)
        case Contains(left: AttributeReference, right: Literal) =>
          val substringComparator = new SubstringComparator(right.value.toString)
          createSingleColumnValueFilter(left, right, CompareOp.EQUAL, substringComparator)
        case _ => None
      }
    }
  }
}

