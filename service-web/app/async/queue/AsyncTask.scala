package async.queue

import java.util.concurrent.atomic.AtomicReference

import async.queue.AsyncQueue.{INIT, INTERRUPTED, TaskStatus}
import log.Logging

abstract class AsyncTask[V](val taskId: String) extends Logging {
  private[queue] final val taskStatus: AtomicReference[TaskStatus] = new AtomicReference[TaskStatus](INIT)

  def onStart()

  def call(): V

  def onSuccess(result: V)

  def onFailure(e:Exception)
}
