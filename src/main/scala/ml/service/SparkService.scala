package ml.service

import javax.ws.rs.Path
import ml.deploy.SparkJob
import javax.ws.rs.GET
import javax.ws.rs.Produces
import org.apache.spark.SparkException
import javax.ws.rs.core.Context
import javax.servlet.http.HttpServletRequest
import org.apache.spark.sql.SparkSession
/**
 * 关闭sparksession job
 */
@Path("/spark")
class SparkService {
  /**
   * 关闭sparksession
   */
  @GET
  @Path("/close")
  @Produces(Array("text/plain;charset=utf8"))
  def close(): String = {
    val spark:SparkSession = SparkJob.getSpark();
    try {
      //取消所有job
      if (spark != null && (!spark.sparkContext.isStopped)) {
        spark.sparkContext.cancelAllJobs();
        //关闭
        spark.close()
      }

    } catch {
      case e: Exception => throw new SparkException(s"Exception when registering SparkListener", e)
    }

    return "job 已经关闭"
  }

  @GET
  @Path("/conf")
  @Produces(Array("text/plain;charset=utf8")) //*MediaType.TEXT_PLAIN,*/ ";charset=utf-8"
  def setConf(@Context request: HttpServletRequest): String = {
    val params = request.getParameterMap();
    val keys = params.keySet().iterator();
    //获取spark conf配置参数
    var map: Map[String, String] = Map()
    while (keys.hasNext()) {
      val key = keys.next();
      map += (key -> request.getParameter(key))
    }
    try {
      SparkJob.setSparkConf(map)
    } catch {
      case e: Exception => {
        new SparkException(s"Exception when registering SparkListener", e).printStackTrace();
        return "fail";
      }
    }
    return "success"
  }
}