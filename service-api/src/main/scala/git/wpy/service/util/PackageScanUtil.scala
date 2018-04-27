package git.wpy.service.util

import java.io.{File, FileInputStream}
import java.net.URL
import java.util.jar.JarInputStream

import org.apache.spark.internal.Logging

import scala.collection.mutable.ArrayBuffer

object PackageScanUtil extends Logging {

  val classLoader = this.getClass.getClassLoader

  def scan(basePackage: String): Seq[Class[_]] = {
    val splashPath = basePackage.replaceAll("\\.", "/")
    val url = classLoader.getResource(splashPath)
    val filePath = getRootPath(url)
    val names = if (filePath.endsWith(".jar")) {
      logInfo(s"scan jar file: $filePath.")
      readFromJarFile(filePath, splashPath)
    } else {
      logInfo(s"scan directory: $filePath")
      readFromDirectory(filePath)
    }

    names.filter(!_.contains("$")).flatMap { file =>
      if (file.endsWith(".class")) Seq(classLoader.loadClass(file))
      else scan(basePackage + "." + file)
    }
  }


  def readFromDirectory(filePath: String) = new File(filePath).list()

  def readFromJarFile(filePath: String, packageName: String) = {
    val jarIn = new JarInputStream(new FileInputStream(filePath))
    var entry = jarIn.getNextJarEntry
    val list = new ArrayBuffer[String]()
    while (entry != null) {
      val name = entry.getName
      if (name.startsWith(packageName) && name.endsWith(".class")) {
        list += name
      }
      entry = jarIn.getNextJarEntry
    }
    list.toArray
  }


  def getRootPath(url: URL) = {
    val fileUrl = url.getFile
    val pos = fileUrl.indexOf('!')
    if (-1 == pos) fileUrl
    else fileUrl.substring(5, pos)
  }

  def isJarFile(filepath: String) = {

  }

}
