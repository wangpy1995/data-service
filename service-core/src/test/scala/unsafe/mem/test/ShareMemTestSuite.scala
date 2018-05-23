package unsafe.mem.test

import java.io.RandomAccessFile
import java.nio.channels.FileChannel

object ShareMemTestSuite {

  val fileName = "/home/wpy/tmp/mem.share"

  val RAFile = new RandomAccessFile(fileName, "r")
  val fc = RAFile.getChannel
  //共享内存大小
  val size = 1024 * 1024 * 1024
  val mapBuffer = fc.map(FileChannel.MapMode.READ_ONLY, 0, size)

  def main(args: Array[String]): Unit = {
    0 until size / 2 foreach { i =>
      println(mapBuffer.getChar(i))
    }
  }

}
