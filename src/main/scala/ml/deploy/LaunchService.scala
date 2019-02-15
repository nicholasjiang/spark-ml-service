package ml.deploy

//import org.apache.logging.log4j.LogManager
//import org.apache.logging.log4j.Logger

import org.apache.log4j.Logger
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.servlet.ServletHolder
import org.glassfish.jersey.server.ServerProperties
import org.glassfish.jersey.servlet.ServletContainer;
/**
 * 启动服务
 */
class LaunchService {
  private val log = Logger.getLogger(classOf[LaunchService])
  def start(port: Int) = {
    
    val server = new Server(port);
    val handler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    handler.setContextPath("/restapi"); //一级目录
    val holder = new ServletHolder(classOf[ServletContainer]);
    holder.setInitParameter(ServerProperties.PROVIDER_PACKAGES, "com.webank.bdp.ml.service");
    handler.addServlet(holder, "/*");
    server.setHandler(handler);
    handler.start();
    server.start();
//    server.join()
    log.info("start ... port: " + port);
    
  }

}
/**
 * 启动服务：1.初始化sparkSession，2.spark 加载算法服务化
 */
object LaunchService {
  private val log = Logger.getLogger(classOf[LaunchService])
  def main(args: Array[String]) {
    log.info("start server....")
    //spark服务初始化
//    SparkJob.getSpark(args)
    val startServer = new Thread(new Runnable {
      @Override
      def run(): Unit = {
        
        val ls = new LaunchService
        if(args==null||args.length==0){
           ls.start(11120)
        } else{
          val port=args(0).trim().toInt
           ls.start(port)
        }
      }
    })
    startServer.start()
  }
}