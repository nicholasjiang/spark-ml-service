package mllib.algorithms
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.classification.{ SVMModel, SVMWithSGD }
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.Path
import java.text.SimpleDateFormat

/**
 * svm 算法分类
 */
class SVM extends AbstractAlgorithm {
 /* *//**
   * 删除文件
   *//*
  def delete(sc: SparkContext, filepath: String) = {
    val hadoopConf = sc.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    val path = new Path(filepath)
    if (hdfs.exists(path)) {
      //为防止误删，禁止递归删除
      hdfs.delete(path, true)
    }
  }
  
  *//**
   * 文件是否存在
   *//*
  def exists(sc: SparkContext, filepath: String):Boolean = {
    val hadoopConf = sc.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    val path = new Path(filepath)
    if (hdfs.exists(path)) {
     return true
    }
   return false
  }*/
  /* 训练数据，生成模型
   *
   */
  @Override
  def trainModel(session: SparkSession, params: Map[String, Any]) {
    val sc = session.sparkContext;
    val train_data = MLUtils.loadLibSVMFile(sc, params.get("input").get.toString())
    train_data.cache()
    val model = SVMWithSGD.train(train_data, params.get("numIterations").get.toString().toInt,
      params.get("stepSize").get.toString().toDouble, params.get("regParam").get.toString().toDouble, params.get("miniBatchFraction").get.toString().toDouble)
    //delete(sc,params.get("modelPath").get.toString())
    if(!exists(sc,params.get("modelPath").get.toString())){
     
    model.save(sc, params.get("modelPath").get.toString())
    }else{
       delete(sc,params.get("modelPath").get.toString())
        model.save(sc, params.get("modelPath").get.toString())
    }
  }
  /* 加载模型,预测数据
   *
   */
  @Override
  def predictModel(session: SparkSession, params: Map[String, Any]) {
    import session.implicits._
    val sc = session.sparkContext;
    val predict_data = MLUtils.loadLibSVMFile(sc, params.get("input").get.toString())
    predict_data.cache()

    val svmModel = SVMModel.load(sc, params.get("modelPath").get.toString())
    val scoreAndLabels = predict_data.map { point =>
      val score = svmModel.predict(point.features)
      (score, point.label)
    }

    val result = scoreAndLabels.map(x => {
      (x._1, x._2)
    }).toDF("prediction", "label")
    result.show(false)
    result.write.mode("overwrite").csv(params.get("predictPath").get.toString())
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
        result = "模型保存路径：" + params.get("modelPath").get.toString() + "\r\nSVM train over\r\n时间："+sdf.format(System.currentTimeMillis())
      case "predict" =>
        predictModel(session, params);
        result = "预测结果路径：" + params.get("predictPath").get.toString() + "\r\nSVM predict resutl save success \r\n时间："+sdf.format(System.currentTimeMillis())
      case _ => result = "error useType，please train or predict"
    }
    result
  }
}