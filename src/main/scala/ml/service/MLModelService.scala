package ml.service

import javax.ws.rs.Path
import javax.ws.rs.GET
import javax.ws.rs.Produces
import javax.ws.rs.POST
import javax.ws.rs.core.Context
import javax.servlet.http.HttpServletRequest

import mllib.algorithms.ModelFactory
/**
 * 算法服务化，支持请类型，get，post
 */
@Path("/ml")//二级目录
class MLModelService {
  @GET
	@Produces(Array("text/plain;charset=utf8"))//*MediaType.TEXT_PLAIN,*/ ";charset=utf-8"
	def serviceGET(@Context  request:HttpServletRequest):String={
    val result=service(request)
    result
  }
  
  @POST
	@Produces(Array("text/plain;charset=utf8"))//*MediaType.TEXT_PLAIN,*/ ";charset=utf-8"
	def servicePOST(@Context  request:HttpServletRequest):String={
      val result=service(request)
      result
  }
  /**
   * 统一服务加载
   */
  def service(request:HttpServletRequest):String={
    val params=request.getParameterMap();
    val keys=params.keySet().iterator();
    var map:Map[String,Any]=Map[String,Any]()
    while(keys.hasNext()){
      val key=keys.next().asInstanceOf[String];
      map += (key->request.getParameter(key))
    }
    val mf =new ModelFactory
        mf.chooseAlgs(map)
   
  }
}