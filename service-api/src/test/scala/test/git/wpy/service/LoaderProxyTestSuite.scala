package test.git.wpy.service

import git.wpy.service.annotation.LoaderCreator
import git.wpy.service.proxy.{LoaderCreatorProxy, LoaderCreatorTest}
import git.wpy.service.util.PackageScanUtil
import org.scalatest.FunSuite

class LoaderProxyTestSuite extends FunSuite {

  val allClass = PackageScanUtil.scan("git.wpy.service.proxy")
  val loaderCreator = allClass.filter(_.getAnnotation(classOf[LoaderCreator]) != null)

  test("creator") {
    loaderCreator.foreach {loader=>
      val test = LoaderCreatorProxy.getProxy(loader)
      test.asInstanceOf[LoaderCreatorTest].createLoader()
    }
  }
}
