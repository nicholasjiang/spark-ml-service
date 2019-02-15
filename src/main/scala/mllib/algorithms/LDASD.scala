package mllib.algorithms

import org.apache.spark.sql.SparkSession

/**
 * 基于主题的聚类
 */
class LDASD extends AbstractAlgorithm {
  case class Params(
    input:       String = null,
    modelPath:   String = null,
    predictPath: String = null,
    K:     Int    = 10,
    maxIter:    Int = 10,
    seed: Long = 0L ,//spark 2.2.0 支持
    checkpointInterval:Int =10,
    Optimizer : String,
    docConcentration : Double
  )

  override def trainModel(session: SparkSession, params: Map[String, Any]) = {

  }
  /**
   *  加载模型
   */
  override def predictModel(session: SparkSession, params: Map[String, Any]) = {

  }

  /**
   * 启动模型训练和预测
   */
  override def run(session: SparkSession, params: Map[String, Any]): String = {
    null
  }
}