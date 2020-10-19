import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.ml._
import co.theasi.plotly
import co.theasi.plotly.{Plot, writer}

object program2 {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  case class Data(date: String, close: Double, volume: Double, open: Double, high: Double, low: Double)

  def main(args: Array[String]) {

  //path to hdfs
    val file: String = "hdfs://"

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Who Wants to be a Millionaire-program1")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._


    var data = sc.textFile(file).mapPartitions(iterator => iterator.drop(1)).filter(_.nonEmpty).map(_.split("[,\\s]")).
      map { p =>
        Data(


          p(0).replaceAll("^\"|\"$", "").replaceAll("^\"N/A\"$", 0.toString),
          p(1).replaceAll("^\"|\"$", "").replaceAll("^N/A", 0.toString).toDouble,
          p(2).replaceAll("^\"|\"$", "").replaceAll("^N/A", 0.toString).toDouble,
          p(3).replaceAll("^\"|\"$", "").replaceAll("^N/A", 0.toString).toDouble,
          p(4).replaceAll("^\"|\"$", "").replaceAll("^N/A", 0.toString).toDouble,
          p(5).replaceAll("^\"|\"$", "").replaceAll("^N/A", 0.toString).toDouble

        )
      }.toDF().withColumn("company", input_file_name())




    data = data.withColumn("company", regexp_replace($"company", lit(".*\\/"), lit("")))

    data = data.withColumn("company", regexp_replace($"company", lit(".dat$|.txt$|.csv$"), lit("")))

    var companies = data.select("company").distinct().withColumnRenamed("company","comp")

    data = data.select(to_date($"date", "yyyy/MM/dd").alias("date"), data.drop("date").col("*"))


    //Get 2018 data
    var processed_data = data.filter($"date".between("2017-12-31", "2019-01-01"))
    processed_data.orderBy("date")

    //var y = processed_data.select(processed_data.col("company")).distinct().groupBy("company").
    //  pivot("company").count()

    //y  = y.withColumnRenamed("company","date")

    //finding average for each day
    //processed_data = processed_data.groupBy("date").agg(mean("close") as("close"),mean("open")as("open"),
    // mean("volume")as("volume"),mean("high")as("high"),mean("low")as("low")).orderBy("date")

    //println("processed_data is:"+ processed_data.count())


    processed_data.filter(processed_data("company") === "zdge").orderBy(processed_data("date"))
    processed_data = processed_data
      .withColumn("MovingClose", avg(processed_data("close")).over(Window.partitionBy("company").orderBy("date").rowsBetween(-3, -1)))
      .withColumn("MovingOpen", avg(processed_data("open")).over(Window.partitionBy("company").orderBy("date").rowsBetween(-3, -1)))
      .withColumn("MovingVolume", avg(processed_data("volume")).over(Window.partitionBy("company").orderBy("date").rowsBetween(-3, -1)))
      .withColumn("MovingHigh", avg(processed_data("high")).over(Window.partitionBy("company").orderBy("date").rowsBetween(-3, -1)))
      .withColumn("MovingLow", avg(processed_data("low")).over(Window.partitionBy("company").orderBy("date").rowsBetween(-3, -1)))


    processed_data.filter(processed_data("company") === "zdge").orderBy(processed_data("date"))


    processed_data.select(mean("MovingClose"), mean("MovingOpen"), mean("MovingVolume"),
      mean("MovingHigh"), mean("MovingLow"), mean("close")).collect()

    val map = Map("MovingClose" -> 84.45311636869235, "MovingOpen" -> 84.43920306506813,
      "MovingVolume" -> 4013262.093598567, "MovingHigh" -> 85.11882976549552,
      "MovingLow" -> 83.71898926767366, "close" -> 84.54328576996772)

    processed_data = processed_data.na.fill(map)

    processed_data = processed_data.select(processed_data.col("close")
      , dayofmonth(processed_data.col("date")).alias("day")
      , month(processed_data.col("date")).alias("month")
      , year(processed_data.col("date")).alias("year")
      , processed_data.col("MovingClose")
      , processed_data.col("MovingOpen")
      , processed_data.col("MovingVolume")
      , processed_data.col("MovingHigh")
      , processed_data.col("MovingLow")
      , processed_data.col("open")
      , processed_data.col("date")
      , processed_data.col("company"))
      .orderBy("date")

    processed_data = processed_data.drop("date")

    val savedmodel: String = "src/main/resources/savedModel/myRandomForestRegressionModel"
    val model = PipelineModel.load(savedmodel)

    val newNames = Seq("label", "features","company")
    val test_data = processed_data.map(x => (
      x.getDouble(0), Vectors.dense(x.getInt(1), x.getInt(2)
      , x.getInt(3), x.getDouble(4), x.getDouble(5), x.getDouble(6)
      , x.getDouble(7), x.getDouble(8), x.getDouble(9)),x.getString(10))).toDF(newNames: _*)


    //test_data.show()


    /*companies.foreach{

      x => x.toSeq.foreach{

        col => {

          test_data.select(test_data.company === 2)
        }

      }



    }*/


    var predictions = model.transform(test_data)
    predictions = predictions.select("prediction", "label", "features","company").toDF()
    val vecToArray = udf( (xs: linalg.Vector) => xs.toArray )
    var dfArr = predictions.withColumn("featuresArr" , vecToArray($"features")).withColumn("prediction",predictions.col("prediction"))
    val elements = Array("day","month","year","MovingClose","MovingOpen","MovingVolume","MovingHigh","MovingLow","open")
    var sqlExpr = elements.zipWithIndex.map{case(alias, idx) => col("featuresArr").getItem(idx).as(alias)}
    sqlExpr = sqlExpr.:+(dfArr.col("prediction"))
    sqlExpr = sqlExpr.:+(dfArr.col("label"))
    sqlExpr = sqlExpr.:+(dfArr.col("company"))

    predictions  = dfArr.select(sqlExpr : _*)

    predictions  = predictions.orderBy("day","month","year","company")

    var  decision = predictions.map{
      x => (theDate(x.getDouble(0).toInt,x.getDouble(1).toInt,x.getDouble(2).toInt),x.getString(11),x.getDouble(9),x.getDouble(10),x.getDouble(8),dec(x.getDouble(8),x.getDouble(9)))
    }

    var final_decision = decision.toDF("string_date","company","prediction","label","open","decision")


    // Select (prediction, true label) and compute test error.
    /* val evaluator = new RegressionEvaluator()
       .setLabelCol("label")
       .setPredictionCol("prediction")
       .setMetricName("rmse")
     val rmse = evaluator.evaluate(predictions)
     println("Root Mean Squared Error (RMSE) on test data = " + rmse)
     */


    final_decision = final_decision.withColumn("date",(to_date($"string_date", "yyyy/MM/dd"))).drop("string_date")

    final_decision =  final_decision.select(final_decision.col("date"),final_decision.col("company"),final_decision.col("open")
      ,final_decision.col("label").as("close"),final_decision.col("prediction"),final_decision.col("decision"))
      .orderBy("date","company")

    final_decision.show()

    var  transactions = final_decision.map{
      x => (x.getDate(0),x.getString(1),x.getDouble(2),x.getDouble(3),x.getDouble(4)
        ,x.getString(5),transact(x.getString(5),x.getDate(0),x.getDouble(2),Investment)
        ,close_Investment(x.getString(5),x.getDouble(2),x.getDouble(3),Investment))
    }.toDF("date","company","open","close","prediction","Decision","transaction","close_Investment")

    transactions.rdd.saveAsTextFile("src/main/resources/transactions")

  }


  var Investment = 10000000.0
  var start = 1
  var current_day = new Date()
  var pre_day = new Date()
  var transact = 0.0

  def dec(open: Double, prediction: Double): String = {
    if (prediction > open)
      return  "Buy"

    else return "dont buy"
  }

  def theDate(day: Int, month: Int, year: Int): String = {

    return year.toString+"/" + "%02d".format(month) + "/" + "%02d".format(day)
  }

  def transact(dec: String, date: Date,open: Double,investment: Double ): Double = {
    if(investment <= 0.0)
      return 0
    if (dec == "dont buy")
    return 0.0
    if(start ==1){
      pre_day = date
      current_day = date
    }
    else{
      current_day = date
    }

    transact = (5000*open)
    return transact

  }


  def close_Investment(dec: String,open: Double,close: Double,investment: Double) : Double = {
    if(investment <= 0.0)
      return 0
    if (dec == "dont buy")
      return Investment
    else
      Investment = Investment + ((5000*close) - (5000*open))
    return Investment
  }


}

