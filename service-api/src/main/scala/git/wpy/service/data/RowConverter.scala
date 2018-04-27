package git.wpy.service.data

import org.apache.spark.sql.Row

//TODO 与schema的顺序一一对应
trait RowConverter[-T] {

  def convert2Row(value: T): Row

}
