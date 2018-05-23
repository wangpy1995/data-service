package git.wpy.service.schema

import org.apache.spark.sql.types.StructType

trait SchemaProvider {
  def createSchema(): StructType

  def tableName: String
}
