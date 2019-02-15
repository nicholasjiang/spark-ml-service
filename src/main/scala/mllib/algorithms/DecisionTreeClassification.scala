package mllib.algorithms

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{ IndexToString, StringIndexer, VectorIndexer }
import org.apache.spark.ml.PipelineModel
import java.text.SimpleDateFormat
import org.apache.spark.sql.SaveMode

/**
 * 决策树分类
 */
class DecisionTreeClassification extends AbstractAlgorithm {
  /**
   * 设置参数
   * MaxCategorie：分裂数据
   * label :分类标签
   * indexedLabel:索引标签
   * features:特征
   * indexedFeatures：索引特征
   * prediction：预测值
   * predictedLabel：真实标注分类值
   * metricName：评估方式， (supports `"f1"` (default), `"weightedPrecision"`,`"weightedRecall"`, `"accuracy"`)
   */

  case class Params(
    maxCategorie:    Int    = 4,
    label:           String = "label",
    indexedLabel:    String = "indexedLabel",
    features:        String = "features",
    indexedFeatures: String = "indexedFeatures",
    prediction:      String = "prediction",
    predictedLabel:  String = "predictedLabel",
    metricName:      String = "f1",
    input:           String = null,
    modelPath:       String = null,
    predictPath:     String = null)
  //设置默认参数
  val default = Params()
  /**
   *  训练模型
   */
  @Override
  def trainModel(session: SparkSession, params: Map[String, Any]) {

    val userParams = Params(
      input = params.getOrElse("input", default.input).toString(),
      maxCategorie = params.getOrElse("maxCategorie", default.maxCategorie).asInstanceOf[Int],
      modelPath = params.getOrElse("modelPath", default.modelPath).toString())
    import session.implicits._
    val input_data = session.read.format("libsvm").load(userParams.input)

    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer()
      .setInputCol(userParams.label)
      .setOutputCol(userParams.indexedLabel)
      .fit(input_data)
    // Automatically identify categorical features, and index them.
    val featureIndexer = new VectorIndexer()
      .setInputCol(userParams.features)
      .setOutputCol(userParams.indexedFeatures)
      .setMaxCategories(userParams.maxCategorie) // features with > 4 distinct values are treated as continuous.
      .fit(input_data)

    // Split the data into training and test sets (30% held out for testing).
    //    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    // Train a DecisionTree model.
    val dt = new DecisionTreeClassifier()
      .setLabelCol(userParams.indexedLabel)
      .setFeaturesCol(userParams.indexedFeatures)

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol(userParams.prediction)
      .setOutputCol(userParams.predictedLabel)
      .setLabels(labelIndexer.labels)

    // Chain indexers and tree in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))

    // Train model. This also runs the indexers.
    val model = pipeline.fit(input_data)
    //保存模型
    if (!exists(session.sparkContext, userParams.modelPath)) {
      //save model
      model.save(userParams.modelPath)
    } else {
      delete(session.sparkContext, userParams.modelPath)
      model.save(userParams.modelPath)
    }
  }
  /**
   *  加载模型
   */
  @Override
  def predictModel(session: SparkSession, params: Map[String, Any]) {
    val userParams = Params(
      input = params.getOrElse("input", default.input).toString(),
      maxCategorie = params.getOrElse("maxCategorie", default.maxCategorie).asInstanceOf[Int],
      modelPath = params.getOrElse("modelPath", default.modelPath).toString(),
      predictPath = params.getOrElse("predictPath", default.predictPath).toString())
    //load testdata
    val predictData = session.read.format("libsvm").load(userParams.input)

    val model = PipelineModel.load(userParams.modelPath)
    val predictions = model.transform(predictData)
    //save model ,type :Overwrite
    predictions.select("predictedLabel", "label", "features").write.mode(SaveMode.Overwrite).csv(userParams.predictPath)
  }

  /**
   * 启动模型训练和预测
   */
  @Override
  def run(session: SparkSession, params: Map[String, Any]): String = {

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