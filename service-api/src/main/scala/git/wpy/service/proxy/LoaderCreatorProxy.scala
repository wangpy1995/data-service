package git.wpy.service.proxy

import java.lang.reflect.Method

import git.wpy.service.annotation.LoaderCreator
import net.sf.cglib.proxy.{Enhancer, MethodInterceptor, MethodProxy}

object LoaderCreatorProxy extends MethodInterceptor {

  private val enhancer = new Enhancer()

  def getProxy(cls: Class[_]) = {
    enhancer.setSuperclass(cls)
    enhancer.setCallback(this)
    enhancer.create()
  }

  override def intercept(obj: Any, method: Method, args: Array[AnyRef], proxy: MethodProxy) = {
    if (!method.isAnnotationPresent(classOf[LoaderCreator]))
      proxy.invokeSuper(obj, args)
    else {
      val clz = method.getAnnotation(classOf[LoaderCreator])
      println(s"start create loader for ${clz.loaderType()}.")
      val result = proxy.invokeSuper(obj, args)
      println("create loader success.")
      result
    }
  }
}
