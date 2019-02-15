package mllib.algorithms

import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.fpm.FPGrowth
import java.text.SimpleDateFormat
import org.apache.spark.mllib.fpm.FPGrowthModel

/**
 * FPG 频繁项集挖掘算法
 *
 */
class FPGrowthSD extends AbstractAlgorithm {
  case class Params(
    input:        String = null,
    predictPath:       String = null,
    modelPath:    String = null,
    minSupport:   Double = 0.3,
    numPartition: Int    = -1)
  //设置默认参数
  val default = Params()
  override def trainModel(session: SparkSession, params: Map[String, Any]) = {
    import session.implicits._
    val userParams = Params(
      input = params.getOrElse("input", default.input).toString(),
      modelPath = params.getOrElse("modelPath", default.modelPath).toString(),
      minSupport = params.getOrElse("minSupport", default.minSupport).asInstanceOf[Double],
      numPartition = params.getOrElse("numPartition", default.numPartition).asInstanceOf[Int])

    val data = session.read.text(userParams.input).rdd.map(x => x.getSeq[String](0).toArray[String]).cache()
    val model = new FPGrowth().setMinSupport(userParams.minSupport).setNumPartitions(userParams.numPartition).run(data)
    //保存模型
    model.save(session.sparkContext, userParams.modelPath)
  }
  /**
   *  加载模型
   */
  override def predictModel(session: SparkSession, params: Map[String, Any]) = {
    import session.implicits._
    val userParams = Params(
      modelPath = params.getOrElse("modelPath", default.modelPath).toString(),
      predictPath = params.getOrElse("predictPath", default.predictPath).toString(),
      minSupport = params.getOrElse("minSupport", default.minSupport).asInstanceOf[Double],
      numPartition = params.getOrElse("numPartition", default.numPartition).asInstanceOf[Int])

    val model =   FPGrowthModel.load(session.sparkContext, userParams.modelPath)
    val analysis_res = model.freqItemsets.map(itemset => (itemset.items.mkString("[", ",", "]"), itemset.freq)).toDF("iterms", "weight")
   //保存输出结果
    analysis_res.write.csv(userParams.predictPath)
//    model.save(session.sparkContext, userParams.modelPath)
  }

  /**
   * 启动模型训练和预测
   */
  override def run(session: SparkSession, params: Map[String, Any]): String = {
    
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val useType = params.get("useType").get.toString()
    var result = ""

    useType match {
      case "train" =>
        trainModel(session, params);
        result = "模型保存路径：" + params.get("modelPath").get.toString() + "\r\nDTC train over\r\n时间：" + sdf.format(System.currentTimeMillis())
      case "predict" =>
        predictModel(session, params);
        result = "预测结果路径：" + params.get("predictPath").get.toString() + "\r\nDTC predict resutl save success \r\n时间：" + sdf.format(System.currentTimeMillis())
      case _ => result = "error useType，please train or predict"
    }
    result
    

  }
}