package ml.deploy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.Builder
import java.security.PrivilegedExceptionAction
import org.apache.hadoop.security.UserGroupInformation
/**
 * job服务监控
 */
object SparkJob {

  private var spark: SparkSession = _;

  private def getSpark(): SparkSession = {
    if (spark == null || spark.sparkContext.isStopped) {

      spark = SparkSession.builder().appName("thriftserver-mlss-di").enableHiveSupport().getOrCreate();
    }
    spark
  }
  /**
   * 验证是否使用代理用户(proxyUser)，
   * 默认是不支
   */
  def getSpark(puser: String = null): SparkSession = {
    if (puser != null) {
      //创建代理用户
      val proxyUser = UserGroupInformation.createProxyUser(puser, UserGroupInformation.getCurrentUser())
      UserGroupInformation.setLoginUser(proxyUser)
      try {
        proxyUser.doAs(new PrivilegedExceptionAction[Unit]() {
          override def run(): Unit = {
            if (spark != null && (!spark.sparkContext.isStopped)) {
              //先关闭才能初始化参数
              spark.close();
            }
            spark = SparkSession.builder().appName("thriftserver-mlss-di").enableHiveSupport().getOrCreate();
          }
        })
      } catch {
        case e: Exception =>
          // Hadoop's AuthorizationException suppresses the exception's stack trace, which
          // makes the message printed to the output by the JVM not very helpful. Instead,
          // detect exceptions with empty stack traces here, and treat them differently.
          if (e.getStackTrace().length == 0) {
            // scalastyle:off println
            System.err.println(s"ERROR: ${e.getClass().getName()}: ${e.getMessage()}")
            // scalastyle:on println
            System.exit(1)
          } else {
            throw e
          }
      }
    } else {
      getSpark()
    }
    spark
  }
  /**
   * 设置Spark Session启动参数
   */
  def setSparkConf(params: Map[String, String]) {
    var builder: Builder = null;
    if (spark != null && (!spark.sparkContext.isStopped)) {
      //先关闭才能初始化参数
      spark.close();
    }
    //设置参数
    val setConf = () => {
      builder = SparkSession.builder().appName("thriftserver-mlss-di")
      params.foreach { case (key, value) => builder = builder.config(key, value) }
      spark = builder.enableHiveSupport().getOrCreate();
    }

    //验证代理用户

    if (params.isDefinedAt("proxy-user")) {
      //创建代理用户
      val proxyUser = UserGroupInformation.createProxyUser(params.get("proxy-user").get, UserGroupInformation.getCurrentUser())
      UserGroupInformation.setLoginUser(proxyUser)
      
      try {
        proxyUser.doAs(new PrivilegedExceptionAction[Unit]() {
          override def run(): Unit = {
            setConf()
          }
        })
      } catch {
        case e: Exception =>
          // Hadoop's AuthorizationException suppresses the exception's stack trace, which
          // makes the message printed to the output by the JVM not very helpful. Instead,
          // detect exceptions with empty stack traces here, and treat them differently.
          if (e.getStackTrace().length == 0) {
            // scalastyle:off println
            System.err.println(s"ERROR: ${e.getClass().getName()}: ${e.getMessage()}")
            // scalastyle:on println
            System.exit(1)
          } else {
            throw e
          }
      }
    } else {
      setConf()
    }
    //创建session 配置参数

  }

  def close = spark.close()
  def main(args: Array[String]) {

    /*    val content = getSpark().sparkContext.textFile("hdfs://10.255.8.42:8020/tmp/bdpml/svm/data/userdata.txt", 5)
    val cnt = content.count()
    println(cnt)
    getSpark().close()
    val content1 = getSpark().sparkContext.textFile("hdfs://10.255.8.42:8020/tmp/bdpml/svm/data/userdata.txt", 5)
    val cnt1 = content1.count()*/
    println(System.getProperty("user.name"))
    println(sys.env.get("user.name"))
    val params: Map[String, String] = Map("c1" -> "u2", "proxy-" -> "user01")
    // println(params.get("proxy-user").get)
    println(params)
  }
}