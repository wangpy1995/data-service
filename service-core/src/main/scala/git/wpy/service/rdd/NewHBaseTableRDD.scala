package git.wpy.service.rdd

import git.wpy.format.SplitableTableInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.NewHadoopRDD

class NewHBaseTableRDD(sc: SparkContext, hbaseConf: Configuration)
  extends NewHadoopRDD(
    sc,
    classOf[TableInputFormat],
    classOf[ImmutableBytesWritable],
    classOf[Result],
    hbaseConf) {

  val credentialUtil = new HBaseCredentialUtil(sc, hbaseConf)

  override def compute(theSplit: Partition, context: TaskContext) = {
    credentialUtil.applyCredentials()
    super.compute(theSplit, context)
  }
}

class HBaseCredentialUtil(@transient sc: SparkContext,
                          @transient hbaseConf: Configuration) extends Serializable with Logging {

  @transient var appliedCredentials = false
  @transient val job = Job.getInstance(hbaseConf)
  TableMapReduceUtil.initCredentials(job)
  @transient var credentials = job.getCredentials
  val credentialsConf = sc.broadcast(new SerializableWritable(job.getCredentials))

  def applyCredentials() = {
    logDebug("appliedCredentials:" + appliedCredentials + ",credentials:" + credentials)
    @transient val ugi = UserGroupInformation.getCurrentUser
    if (!appliedCredentials && credentials != null) {
      appliedCredentials = true
      ugi.addCredentials(credentials)
    } else {
      credentials = credentialsConf.value.value
      // specify that this is a proxy user
      ugi.setAuthenticationMethod(AuthenticationMethod.PROXY)

      ugi.addCredentials(credentialsConf.value.value)
    }
  }
}
