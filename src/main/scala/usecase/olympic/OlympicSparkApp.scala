package usecase.olympic

import org.apache.spark.sql.SparkSession


object OlympicSparkApp {

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder
      .master("local")
      .appName("SparkHadoopIntegration")
      .config("spark.some.config.options", "some-value")
      .getOrCreate()


    ss.sparkContext.setLogLevel("WARN")
    val textFile = ss.sparkContext.textFile("D:\\Spark\\Usecase\\Olympics\\olympics.csv")

    //textFile.take(10).foreach(println)

    val counts = textFile.filter {
    x => { if (x.toString.split("\t").length >= 10) true else false}}.map(line => line.toString.split("\t"))

    val fil = counts.filter{
      x=> { if (x(5).equalsIgnoreCase("Swimming")  && (x(9).matches("\\d+"))) true else false}
    }

    val pair = fil.map(x=> (x(2),x(9).toInt))
    val cnt = pair.reduceByKey(_+_).collect()

    cnt.take(15).foreach(println)

  }
}
