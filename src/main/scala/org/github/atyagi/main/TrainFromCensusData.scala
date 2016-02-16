package org.github.atyagi.main

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.github.atyagi.structures.AutoIncrementingHashMap

object TrainFromCensusData {

  val TRAINING_DATA_FILE = "/dataset/adult.data"
  val TEST_DATA_FILE = "/dataset/adult.test"

  val STRING_DELIMITER = ","

  val workClass = new AutoIncrementingHashMap
  val education = new AutoIncrementingHashMap
  val maritalStatus = new AutoIncrementingHashMap
  val occupation = new AutoIncrementingHashMap
  val relationship = new AutoIncrementingHashMap
  val race = new AutoIncrementingHashMap
  val sex = new AutoIncrementingHashMap
  val nativeCountry = new AutoIncrementingHashMap

  def main(args: Array[String]) {
    val sc = initializeSpark
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    def splitCSV: (String) => Array[String] = _.split(STRING_DELIMITER)

    val trainingDataSet = sc.textFile(TRAINING_DATA_FILE)
      .map(splitCSV).map(vectorizeData)
      .toDF("salary", "attributes")

    val testDataSet = sc.textFile(TEST_DATA_FILE).map(splitCSV).map(vectorizeData)
      .toDF("salary", "attributes")

    val labelIndexer = new StringIndexer()
      .setInputCol("salary").setOutputCol("indexedSalary")
      .fit(trainingDataSet)

    val featureIndexer = new VectorIndexer()
      .setInputCol("attributes").setOutputCol("indexedAttributes")
      .setMaxCategories(10)
      .fit(trainingDataSet)

    val rf = new RandomForestClassifier()
      .setLabelCol("indexedSalary")
      .setFeaturesCol("indexedAttributes")
      .setNumTrees(20).setMaxDepth(15)

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedSalary")
      .setLabels(labelIndexer.labels)

    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

    val model = pipeline.fit(trainingDataSet)

    val predictions = model.transform(testDataSet)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedSalary")
      .setPredictionCol("prediction")
      .setMetricName("precision")

    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))
  }

  def vectorizeData: Array[String] => (String, Vector) = p => {
    (p(14),
      Vectors.dense(Array(
        p(0).toDouble,
        workClass.addAndGet(p(1)).toDouble,
        p(2).toDouble,
        education.addAndGet(p(3)).toDouble,
        p(4).toDouble,
        maritalStatus.addAndGet(p(5)).toDouble,
        occupation.addAndGet(p(6)).toDouble,
        relationship.addAndGet(p(7)).toDouble,
        race.addAndGet(p(8)).toDouble,
        sex.addAndGet(p(9)).toDouble,
        p(10).toDouble,
        p(11).toDouble,
        p(12).toDouble,
        nativeCountry.addAndGet(p(13)).toDouble
      )))
  }

  def initializeSpark: SparkContext = {
    System.setProperty("hadoop.home.dir", "/Users/atyagi/Code/spark-predict-income-example")
    println(System.getProperty("hadoop.home.dir"))

    val conf = new SparkConf().setAppName("IncomePredictor")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.executor.memory", "4g")
      .setMaster("local[*]")

    new SparkContext(conf)
  }

}
