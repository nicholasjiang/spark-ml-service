package mllib.algorithms

import org.apache.spark.sql.SparkSession
import ml.deploy.SparkJob

 class ModelFactory {
  /**
   * 选择不同的算法
   */
  def chooseAlgs(params:Map[String,Any]):String={
    val alg_name=params.get("algname").get.toString()
     var proxyUser:String=null
    //只支持代理用户执行
    if(params.isDefinedAt("proxy-user")){
     proxyUser= params.get("proxy-user").get.toString()
    }
    val spark:SparkSession=SparkJob.getSpark(proxyUser)
    println(alg_name)
    alg_name match {
    case "svm" => {//支持向量机分类
      val svm= new SVM()
       
       svm.run(spark, params)
      }
    case "lr" => {//逻辑回归
       val lr= new LogisticRegressionLBFGS()
       
        lr.run(spark, params)
      }
     case "als" => {//交替最小二乘法
      val als= new ALSAlg()
       
       als.run(spark, params)
      }
    case "dtc" => {//决策树分类
       val dtc= new DecisionTreeClassification()
       
        dtc.run(spark, params)
      }
    case _ => s"not find $alg_name algorithm"
  }
  }
}
