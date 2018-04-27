import org.apache.spark.rdd.RDD

import scala.beans.BeanProperty
import java.{util=>ju}
class TestBeanProperty {

  @BeanProperty var hotData:List[RDD[Any]] = _
  @BeanProperty var coldData:List[RDD[Any]] = _
  @BeanProperty var parallelism:Int = _

  def this(hotData:List[RDD[Any]],coldData:List[RDD[Any]],parallelism:Int){
    this()
    this.coldData = coldData
    this.hotData = hotData
    this.parallelism = parallelism
  }
}
