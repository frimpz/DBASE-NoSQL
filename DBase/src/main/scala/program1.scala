import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.{FeatureHasher, HashingTF, StandardScaler, StringIndexer, VectorIndexer}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object program1 {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)


  case class Data(date: String, close: Double, volume: Double, open: Double, high: Double, low: Double)
  //case class Data(date: String,stock: String, close: Double, volume: Double, open: Double, high: Double, low: Double)

  def main(args: Array[String]) {


	//path to hdfs	
    val file: String = "hdfs://"

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Who Wants to be a Millionaire-program1")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._


    var data = sc.textFile(file).mapPartitions(iterator =>iterator.drop(1)).filter(_.nonEmpty).map(_.split("[,\\s]")).
      map{p => Data(


        p(0).replaceAll("^\"|\"$","").replaceAll("^\"N/A\"$",0.toString),
        p(1).replaceAll("^\"|\"$","").replaceAll("^N/A",0.toString).toDouble,
        p(2).replaceAll("^\"|\"$","").replaceAll("^N/A",0.toString).toDouble,
        p(3).replaceAll("^\"|\"$","").replaceAll("^N/A",0.toString).toDouble,
        p(4).replaceAll("^\"|\"$","").replaceAll("^N/A",0.toString).toDouble,
        p(5).replaceAll("^\"|\"$","").replaceAll("^N/A",0.toString).toDouble

    )}.toDF()

    data = data.select(to_date($"date","yyyy/MM/dd").alias("date"),data.drop("date").col("*"))

    //Train test Split
    var processed_data = data.filter($"date".between("2016-12-31", "2018-01-01"))
    //processed_data.orderBy("date").show(25,false)
    println("trainingSet is:"+processed_data.count())



    //finding average for each day
    processed_data = processed_data.groupBy("date").agg(mean("close") as("close")
      ,mean("open")as("open"),
     mean("volume")as("volume"),mean("high")as("high")
      ,mean("low")as("low")).orderBy("date")
    //println("processed_data is:"+ processed_data.count())

    processed_data = processed_data.withColumn("MovingClose",avg(processed_data("close")).over(Window.orderBy("date")
      .rowsBetween(-3,-1))).withColumn("MovingOpen",avg(processed_data("open")).over(Window.orderBy("date")
      .rowsBetween(-3,-1))).withColumn("MovingVolume",avg(processed_data("volume")).over(Window.orderBy("date")
      .rowsBetween(-3,-1))).withColumn("MovingHigh",avg(processed_data("high")).over(Window.orderBy("date")
      .rowsBetween(-3,-1))).withColumn("MovingLow",avg(processed_data("low")).over(Window.orderBy("date")
      .rowsBetween(-3,-1)))


    processed_data.show()


    processed_data.select(mean("MovingClose"), mean("MovingOpen"),mean("MovingVolume"),
      mean("MovingHigh"),mean("MovingLow"),mean("close")).collect()

    val map = Map("MovingClose" -> 84.45311636869235, "MovingOpen" -> 84.43920306506813,
      "MovingVolume" -> 4013262.093598567,"MovingHigh" -> 85.11882976549552,
    "MovingLow" -> 83.71898926767366,"close" -> 84.54328576996772)

    processed_data = processed_data.na.fill(map)

    processed_data.show(10)

    processed_data =  processed_data.select(
       processed_data.col("close")
      ,dayofmonth(processed_data.col("date")).alias("day")
      ,month(processed_data.col("date")).alias("month")
      ,year(processed_data.col("date")).alias("year")
      ,processed_data.col("MovingClose")
      ,processed_data.col("MovingOpen")
      ,processed_data.col("MovingVolume")
      ,processed_data.col("MovingHigh")
      ,processed_data.col("MovingLow")
      ,processed_data.col("open")
      ,processed_data.col("date"))


   // processed_data = processed_data.drop("date")


    var Array(trainingData, testData) = processed_data.randomSplit(Array(0.7, 0.3))

    trainingData.orderBy("date").show()

    trainingData.show()

    val newNames = Seq("label", "features")
    trainingData = trainingData.map(x => (x.getDouble(0), Vectors.dense(x.getInt(1),x.getInt(2),x.getInt(3)
      ,x.getDouble(4),x.getDouble(5),x.getDouble(6),x.getDouble(7),x.getDouble(8),x.getDouble(9)))).toDF(newNames:_*)

    testData = testData.map(x => (x.getDouble(0), Vectors.dense(x.getInt(1),x.getInt(2),x.getInt(3)
      ,x.getDouble(4),x.getDouble(5),x.getDouble(6),x.getDouble(7),x.getDouble(8),x.getDouble(9)))).toDF(newNames:_*)


    val features = new VectorIndexer()
      .setInputCol("features")
      //.setOutputCol("features")
      .setMaxCategories(10)
      .fit(trainingData)


    // Train a RandomForest model.
    val rf = new RandomForestRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(30)


   // Chain indexers and forest in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array( features, rf))

    // Train model. This also runs the indexers.
    val model = pipeline.fit(trainingData)


    val rfModel = model.stages(1).asInstanceOf[RandomForestRegressionModel]
    println(rfModel.featureImportances)




    // Make predictions.
    val predictions = model.transform(testData)


    predictions.select("prediction", "label", "features").show(20)

    // Select (prediction, true label) and compute test error.
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)

    /*val rfModel = model.stages(1).asInstanceOf[RandomForestRegressionModel]
    println("Learned regression forest model:\n" + rfModel.toDebugString)*/


    // Save and load model
    val savedmodel: String = "src/main/resources/savedModel/myRandomForestRegressionModel"
    model.write.overwrite().save(savedmodel)



  }






}

