package mllib.algorithms

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.Path
import java.text.SimpleDateFormat

abstract class AbstractAlgorithm {
  /**
   *  训练模型
   */
  def trainModel(session: SparkSession, params: Map[String, Any])
  /**
   *  加载模型
   */
  def predictModel(session: SparkSession, params: Map[String, Any])

  /**
   * 启动模型训练和预测
   */
  def run(session: SparkSession, params: Map[String, Any]): String

  /**
   * 评估模型
   */
  def evaluateModel(session: SparkSession, params: Map[String, Any]) {}
  /**
   * 删除文件
   */
  def delete(sc: SparkContext, filepath: String) = {
    val hadoopConf = sc.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    val path = new Path(filepath)
    if (hdfs.exists(path)) {
      //为防止误删，禁止递归删除
      hdfs.delete(path, true)
    }
  }

  /**
   * 文件是否存在
   */
  def exists(sc: SparkContext, filepath: String): Boolean = {
    val hadoopConf = sc.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    val path = new Path(filepath)
    if (hdfs.exists(path)) {
      return true
    }
    return false
  }
}