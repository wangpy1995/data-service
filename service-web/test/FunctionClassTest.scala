import org.scalatest.FunSuite

import scala.util.Random

class FunctionClassTest extends FunSuite {


  test("func") {
    val func = new FuncTest(getInt)
    func.compute(1,2,3,4,5)
    func.compute(1,2,3,4,5)
  }

  def getInt() = {
    val a = Random.nextInt()
    println(s"start ...$a")
    a
  }

  class FuncTest(f: () => Int) {
    def compute(z: Int*): Unit = {
      val x = f()
      z.foreach(println)
      println(s"complete ...$x")
    }
  }

}
