package mllib.algorithms
/**
 * ALS 矩阵分解模型的损失函数
 * ALS算法是基于模型的推荐算法。起基本思想是对稀疏矩阵进行模型分解，评估出缺失项的值，以此来得到一个基本的训练模型。然后依照此模型可以针对新的用户和物品数据进行评估。
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.annotation.Since
import java.text.SimpleDateFormat

class ALSAlg extends AbstractAlgorithm {

  case class Params(
    maxIter:           Int    = 5,
    regParam:          Double = 0.01,
    input:             String = null,
    modelPath:         String = null,
    predictPath:       String = null,
    coldStartStrategy: String = "drop" //spark 2.2.0 支持
  )
  //
  case class Rating(userId: Int, entityID: Int, rating: Float, timestamp: Long)

  def parseRating(str: String): Rating = {

    val fields = str.split("::")
    assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }

  @Override
  def trainModel(session: SparkSession, params: Map[String, Any]) {
    import session.implicits._
    val sc = session.sparkContext
    val param = Params(
      params.get("maxIter").get.toString().toInt,
      params.get("regParam").get.toString().toDouble,
      params.get("input").get.toString(),
      params.get("modelPath").get.toString())
    val als = new ALS()
      .setMaxIter(param.maxIter)
      .setRegParam(param.regParam)
      .setUserCol("userId")
      .setItemCol("entityID")
      .setRatingCol("rating")
    val trainData = session.read.textFile(param.input).map(parseRating).toDF()
    val model = als.fit(trainData)
    //保存模型
    if (!exists(session.sparkContext, param.modelPath)) {
      model.save(param.modelPath)
    } else {
      delete(sc, param.modelPath)
      model.save(param.modelPath)
    }

  }
  /**
   *  加载模型
   */
  @Override
  def predictModel(session: SparkSession, params: Map[String, Any]) {
    import session.implicits._
    val param = Params(
      input = params.get("input").get.toString(),
      modelPath = params.get("modelPath").get.toString(),
      predictPath = params.get("predictPath").get.toString())
    val testData = session.read.textFile(param.input).map(parseRating).toDF()
    val model = ALSModel.load(param.modelPath)
    val result = model.transform(testData)
    //保存预测结果
    result.write.mode("overwrite").csv(param.predictPath)

  }

  /**
   * 启动模型训练和预测
   */
  @Override
  def run(session: SparkSession, params: Map[String, Any]): String = {
    val sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val useType = params.get("useType").get.toString()
    var result = ""
    useType match {
      case "train" =>
        trainModel(session, params); 
        result = "模型保存路径：" + params.get("modelPath").get.toString() + "\r\nALS train over\r\n时间："+sdf.format(System.currentTimeMillis())
      case "predict" =>
        predictModel(session, params);
        result = "预测结果路径：" + params.get("predictPath").get.toString() + "\r\nALS predict resutl save success \r\n时间："+sdf.format(System.currentTimeMillis())
      case _ => result = "error useType，please train or predict"
    }
    result
  }
}