package mllib.algorithms

import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.classification.{ LogisticRegressionModel, LogisticRegressionWithLBFGS }
import org.apache.spark.mllib.regression.LabeledPoint
import java.text.SimpleDateFormat

class LogisticRegressionLBFGS extends AbstractAlgorithm {
  @Override
  def trainModel(session: SparkSession, params: Map[String, Any]) {
    params.map{
      case (key,v) =>println(key)
      case _ =>""
      }
    val NumClasses = params.get("NumClasses").get.toString().toInt
    val data = MLUtils.loadLibSVMFile(session.sparkContext, params.get("input").get.toString())
    val temp_data = data.cache();
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(NumClasses)
      .run(temp_data)
    val sc = session.sparkContext
    val path = params.get("modelPath").get.toString();
    if (exists(sc, path)) {
      delete(sc, path)
      model.save(sc, params.get("modelPath").get.toString())
    } else {
      model.save(sc, params.get("modelPath").get.toString())
    }
  }
  // 加载模型
  @Override
  def predictModel(session: SparkSession, params: Map[String, Any]) {
    import session.implicits._
    val sc=session.sparkContext
    val modelPath=params.get("modelPath").get.toString()
    val input=params.get("input").get.toString()
    
     val test_data = MLUtils.loadLibSVMFile(sc,input)
     val predictPath=params.get("predictPath").get.toString()
     val model = LogisticRegressionModel.load(sc,modelPath)//逻辑回归
     val predictionAndLabels = test_data.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }
    val result=predictionAndLabels.toDF("predict","label");
    result.show(false)
    result.write.mode("overwrite").csv(predictPath)//保存，模型
  }
  @Override
  def run(session: SparkSession, params: Map[String, Any]): String = {
    val sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val useType = params.get("useType").get.toString()
    val algname = params.get("algname").get.toString()
    var result = ""
    useType match {
      case "train" =>
        trainModel(session, params); result = "模型保存路径：" + params.get("modelPath").get.toString() + "\r\n"+algname.toUpperCase()+" train over\r\n时间："+sdf.format(System.currentTimeMillis())
      case "predict" =>
        predictModel(session, params); result = "预测结果路径：" + params.get("predictPath").get.toString() + "\r\n"+algname.toUpperCase()+" predict resutl save success \r\n时间："+sdf.format(System.currentTimeMillis())
      case _ => result = "error useType，please train or predict"
    }
    result
   
  }
}