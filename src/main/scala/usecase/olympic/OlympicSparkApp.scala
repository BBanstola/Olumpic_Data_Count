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

    // Problem Statement 1: Find the total number of medals won by each country in swimming.

    println("Find the total number of medals won by each country in swimming.")

    val fil = counts.filter{
      x=> { if (x(5).equalsIgnoreCase("Swimming")  && (x(9).matches("\\d+"))) true else false}
    }

    val pair = fil.map(x=> (x(2),x(9).toInt))
    val cnt = pair.reduceByKey(_+_).collect()

    cnt.take(15).foreach(println)

    // Problem Statement 2 : Find the number of medals that India won year wise.

    println("Find the number of medals that India won year wise.")

    val fil2 = counts.filter{
      x => {if (x(2).equalsIgnoreCase("India") && (x(9).matches("\\d+"))) true else false}
    }

    val pair2 = fil2.map(x=> (x(3),x(9).toInt))
    val cnt2 = pair2.reduceByKey(_+_).collect()

    println("India won " + fil2.count + " medals in total")
    cnt2.take(5).foreach(println)

    // Problem Statement 3: Find the total number of medals won by each country.

    println("Find the total number of medals won by each country")
    val fil3 = counts.filter{
      x => {if (x(9).matches("\\d+")) true else false}
    }

    val pair3 = fil3.map(x=> (x(2),x(9).toInt))
    val cnt3 = pair3.reduceByKey(_+_).collect()

    cnt3.foreach(println)

  }
}
