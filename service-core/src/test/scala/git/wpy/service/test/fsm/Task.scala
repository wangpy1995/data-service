package git.wpy.service.test.fsm

import java.util.concurrent.Executors

class Task(id: String) extends Runnable {
//  var state: State = null

  override def run(): Unit = {
    println("cs: RUNNING; ns: FINISH")
    println(s"run task: $id")
  }
}

class MyThreadPool {

  val threadPool = Executors.newFixedThreadPool(10)

  def submit(task: Task): Unit = {

    threadPool.submit(task)
  }

}
