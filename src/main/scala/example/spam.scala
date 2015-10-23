//import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{ LogisticRegressionWithLBFGS, LogisticRegressionModel }
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import java.lang.Double
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{ SVMModel, SVMWithSGD }
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.rdd._
import example.ExampleUtils._
import org.apache.spark.mllib.feature.StandardScaler

object ClassificationExample {

  case class ClassificationArgs(
    spamDataFile: String = "",
    model: String = "")

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[ClassificationArgs]("Spam Classification") {
      head("Spam Classification", "1.0")
      opt[String]('s', "spamDataFile") required () action { (x, c) =>
        c.copy(spamDataFile = x)
      } text ("spamDataFile is the modelFile directory")
      opt[String]('m', "model") required () action { (x, c) =>
        c.copy(model = x)
      } text ("the model used to do the classification")
      help("help") text ("prints this usage text")
    }
    var model: (SparkContext, RDD[LabeledPoint], RDD[LabeledPoint]) => Unit = null
    parser.parse(args, new ClassificationArgs()) match {
      case Some(cargs) => {
        model = cargs.model match {
          case "logisticModel" =>
            logisticModel
          case "logisticModel" =>
            SVMModel
        }
        val sc = getSparkContext()
        val (trainData, testData) = loadData(sc, cargs.spamDataFile)
        model(sc, trainData, testData)
      }
    }
  }

  //"/Users/xiafan/Documents/dataset/spambase/spambase.data"
  def loadData(sc: SparkContext, dataFile: String): (RDD[LabeledPoint], RDD[LabeledPoint]) = {
    //数据加载
    val inputTable = sc.textFile(dataFile)
    val mldata = inputTable.map(r => {
      val parts = r.split(",")
      val featureLen = parts.length - 1
      LabeledPoint(Double.parseDouble(parts(featureLen)), Vectors.dense(parts.slice(0, featureLen).map(x => Double.parseDouble(x))))
    })
    mldata.cache()
    val splits = mldata.randomSplit(Array(0.6, 0.4), seed = 11L)
    val trainingData = splits(0)
    val testData = splits(1)
    return (trainingData, testData)
  }

  def logisticModel(sc: SparkContext, trainingData: RDD[LabeledPoint], testData: RDD[LabeledPoint]): Unit = {
    //设置为2分类问题
    val model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(trainingData)

    //在测试集上面查看测试情况
    val labelAndPreds = testData.map(point => {
      val prediction = model.predict(point.features)
      (point.label, prediction)
    })

    val precision = 1 - labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count
  }

  def SVMModel(sc: SparkContext, trainingData: RDD[LabeledPoint], testData: RDD[LabeledPoint]): Unit = {
    // Run training algorithm to build the model
    val numIterations = 100
    val model = SVMWithSGD.train(trainingData, numIterations)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = testData.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    val precision = 1 - scoreAndLabels.filter(r => r._1 != r._2).count.toDouble / testData.count
    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    //Save and load model
    model.save(sc, "path")
    val readModel = LogisticRegressionModel.load(sc, "path")
  }

  def SVMModelWithScaler(sc: SparkContext, trainingData: RDD[LabeledPoint], testData: RDD[LabeledPoint]): Unit = {
    // Run training algorithm to build the model
    val numIterations = 100
    val scaler = new StandardScaler(withMean = true, withStd = true).fit(trainingData.map(x => x.features))
    val scaledTrainingData = trainingData.map(x => (x.label, scaler.transform(x.features)))
    val model = SVMWithSGD.train(trainingData, numIterations)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = testData.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    val precision = 1 - scoreAndLabels.filter(r => r._1 != r._2).count.toDouble / testData.count
    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    //Save and load model
    model.save(sc, "path")
    val readModel = LogisticRegressionModel.load(sc, "path")
  }

  def randomForest(sc: SparkContext, trainingData: RDD[LabeledPoint], testData: RDD[LabeledPoint]): Unit = {
    //随机森林
    // Train a RandomForest model.
    val treeStrategy = Strategy.defaultStrategy("Classification")
    val numTrees = 3 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val model = RandomForest.trainClassifier(trainingData,
      treeStrategy, numTrees, featureSubsetStrategy, seed = 12345)
    // Compute raw scores on the test set.
    val scoreAndLabels = testData.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    val precision = 1 - scoreAndLabels.filter(r => r._1 != r._2).count.toDouble / testData.count
  }
}





