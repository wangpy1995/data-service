package async.task

import async.queue.AsyncTask

class StringTask(id: Int) extends AsyncTask[String](id.toString) {
  override def onStart(): Unit = {
    println(s"task:$taskId starting")
    val cur = System.currentTimeMillis()
    while (System.currentTimeMillis() - cur < 1000) {
    }
  }

  def call(): String = {
    println(s"taskId is: $taskId")
    val cur = System.currentTimeMillis()
    while (System.currentTimeMillis() - cur < 1000) {
    }
    taskId
  }

  override def onSuccess(result: String): Unit = {
    println(s"result is: $result.")
  }

  override def onFailure(e: Exception): Unit = e match {
    case _: InterruptedException =>
      println(s"task[$taskId] canceled.")
    case e =>
      println(s"task[$taskId] failed.")

  }
}
