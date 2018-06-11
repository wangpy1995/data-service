package git.wpy.service.test

import java.io.{File, FileWriter}
import java.util

import git.wpy.bean.{ColumnFamily, Qualifier}
import git.wpy.service.relation.HBaseConstants
import org.scalatest.FunSuite
import org.yaml.snakeyaml.Yaml

class HBaseConstantsSuite extends FunSuite {

  test("writeSchema") {
    val list = new java.util.ArrayList[ColumnFamily]()
    0 until 2 foreach { i =>
      val col = new ColumnFamily()
      col.setName(s"$i")
      val qList = new util.ArrayList[Qualifier]()
      0 until 10 foreach { j =>
        val q = new Qualifier()
        q.setName(s"$j")
        q.setDataType("xxx")
        qList.add(q)
      }
      col.setQualifierList(qList)
      list.add(col)
    }
    val file = new File("test.yml")
    if (!file.exists()) file.createNewFile()
    println(file.getAbsolutePath)
    new Yaml().dump(list.iterator(), new FileWriter(file))
  }

  test("readSchema") {
    val cf = HBaseConstants.createColumnFamilies("/test_schema.yml")
    println(cf)
  }

}
