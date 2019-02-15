package mllib.algorithms
import org.apache.spark.sql.SparkSession
/**
 * 线性回归
 */
class LinearRegression extends AbstractAlgorithm {

  override def trainModel(session: SparkSession, params: Map[String, Any]) = ???
  /**
   *  加载模型
   */
  override def predictModel(session: SparkSession, params: Map[String, Any]) = ???

  /**
   * 启动模型训练和预测
   */
  override def run(session: SparkSession, params: Map[String, Any]): String = ???
}