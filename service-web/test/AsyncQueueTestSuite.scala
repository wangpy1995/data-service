import async.queue.AsyncQueue
import async.task.StringTask
import org.scalatest.FunSuite


class AsyncQueueTestSuite extends FunSuite {

  val queue = new AsyncQueue[String](10)

  test("async") {
    val tasks = (0 until 10).map(new StringTask(_))
    queue.start()
    tasks.foreach(queue.submitTask(_))
    Thread.sleep(100)
    queue.cancelTask(tasks(0))
    Thread.sleep(2000)
    queue.cancelTask(tasks(9))
    queue.stop()
    tasks.foreach(t => println(queue.getResult(t.taskId)))
    queue.start()
    Thread.sleep(5000)
    tasks.foreach(t => println(queue.getResult(t.taskId)))
    Thread.sleep(5000)
    tasks.foreach(t => println(queue.getResult(t.taskId)))
    Thread.sleep(6000)
    tasks.foreach(t => println(queue.getResult(t.taskId)))
  }
}