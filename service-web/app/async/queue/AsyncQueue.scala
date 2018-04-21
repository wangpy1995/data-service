package async.queue

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean

import async.queue.AsyncQueue._
import com.google.common.cache.CacheBuilder
import log.Logging

class AsyncQueue[V <: AnyRef](resultSize: Int) extends Logging {

  private val taskQueue = new ConcurrentLinkedQueue[AsyncTask[V]]()

  private val resultCache = CacheBuilder.newBuilder()
    .maximumSize(resultSize)
    .concurrencyLevel(2)
    .build[String, V]()

  private val started = new AtomicBoolean(false)

  private var thread: Thread = _

  def submitTask(task: AsyncTask[V]) = {
    if (task.taskStatus.compareAndSet(INIT, SUBMITTED)) {
      logInfo(s"submit task[${task.taskId}] success.")
      taskQueue.add(task)
    } else {
      logError(s"submit task[${task.taskId}] failed: expected task status: INIT, actual: ${task.taskStatus.get()}.")
    }
  }

  def getResult(taskId: String) = {
    if (taskId == null) {
      logError("task id is null.")
      null
    } else if (!resultCache.asMap().containsKey(taskId)) {
      logInfo(s"task[$taskId] not exists or removed.")
      null
    } else
      resultCache.getIfPresent(taskId)
  }

  def cancelTask(task: AsyncTask[V]) = task.taskStatus.get() match {
    case INIT =>
      logError(s"cancel task[${task.taskId}] failed: cannot cancel a task that is not submitted.")
    case SUBMITTED =>
      if (task.taskStatus.compareAndSet(SUBMITTED, INTERRUPTED)) {
        logInfo(s"task not start, remove task[${task.taskId}] from waiting queue.")
        taskQueue.remove(task)
      } else {
        logError(s"error while cancel task[${task.taskId}], expected task state: SUBMITTED, actual ${task.taskStatus.get()}")
      }
    case STARTING =>
      if (task.taskStatus.compareAndSet(STARTING, INTERRUPTED)) {
        logWarning(s"try interrupt current task[${task.taskId}], task state: STARTING.")
      } else {
        logError(s"error while cancel task[${task.taskId}], expected task state: STARTING, actual: ${task.taskStatus.get()}")
      }
    case RUNNING =>
      if (task.taskStatus.compareAndSet(STARTING, INTERRUPTED)) {
        logWarning(s"try interrupt current task[${task.taskId}], task state: RUNNING.")
      } else {
        logError(s"error while cancel task[${task.taskId}], expected task state: RUNNING, actual: ${task.taskStatus.get()}")
      }
    case INTERRUPTED =>
      logError(s"cancel task[${task.taskId}] failed: cannot cancel a task that is interrupted.")
    case COMPLETE =>
      logError(s"cancel task[${task.taskId}] failed, task already completed.")
    case FAILED =>
      logError(s"cancel task[${task.taskId}] failed, task already failed.")
  }

  def start() = if (started.compareAndSet(false, true)) this.synchronized {
    if (thread != null)
      while (thread.isAlive) {}
    thread = createThread
    thread.start()
    logInfo("###########async queue started############")
  }

  def stop() = if (started.compareAndSet(true, false)) this.synchronized {
    while (thread.isAlive) {}
    logInfo("############async queue now stopped#########")
  }

  private def createThread = new Thread(new Runnable {
    override def run(): Unit = {
      while (started.get()) {
        if (!taskQueue.isEmpty) {
          val task = taskQueue.poll()
          val id = task.taskId
          try {
            if (task.taskStatus.compareAndSet(SUBMITTED, STARTING)) {
              logInfo(s"starting task[$id]")
              task.onStart()
              if (task.taskStatus.get() == INTERRUPTED) throw new InterruptedException(s"interrupt task:$id")
              if (task.taskStatus.compareAndSet(STARTING, RUNNING)) {
                logInfo(s"running task:[$id]")
                if (task.taskStatus.get() == INTERRUPTED) throw new InterruptedException(s"interrupt task:$id")
                val res = task.call()
                if (task.taskStatus.get() == INTERRUPTED) throw new InterruptedException(s"interrupt task:$id")

                if (task.taskStatus.compareAndSet(RUNNING, COMPLETE)) {
                  logInfo(s"finish task:[$id]")
                  resultCache.put(task.taskId, res)
                  task.onSuccess(res)
                } else {
                  throw new IllegalStateException(s"error while finish task[$id], illegal task state, expected: RUNNING, actual: ${task.taskStatus.get()}")
                }
              } else {
                throw new IllegalStateException(s"error while running task[$id], illegal task state, expected: STARTING, actual: ${task.taskStatus.get()}")
              }
            } else {
              throw new IllegalStateException(s"error while start task[$id], illegal task state, expected: SUBMITTED, actual: ${task.taskStatus.get()}")
            }
          } catch {
            case e: InterruptedException =>
              task.taskStatus.set(INTERRUPTED)
              task.onFailure(e)
              logWarning(s"task[$id] interrupted.")
            case e: Exception =>
              task.taskStatus.set(FAILED)
              task.onFailure(e)
              logWarning(s"task[$id] failed, cause: ${e.getMessage}")
              e.printStackTrace(System.err)
          }
        }
      }
    }
  })

}

private[queue] object AsyncQueue {

  abstract sealed class TaskStatus

  case object SUBMITTED extends TaskStatus

  case object STARTING extends TaskStatus

  case object RUNNING extends TaskStatus

  case object COMPLETE extends TaskStatus

  case object FAILED extends TaskStatus

  case object INIT extends TaskStatus

  case object INTERRUPTED extends TaskStatus

}