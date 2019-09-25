package itldw

import org.apache.spark._

object WordCount {

  def main(args: Array[String]): Unit = {


    val conf=new SparkConf().setAppName("wc")
    val sc=new SparkContext(conf)
    val inputFile="/usr/input.txt"
    var txtData=sc.textFile(inputFile)

    var counts=txtData.flatMap(_.split(" "))
      .filter(_.nonEmpty).map((_,1)).reduceByKey(_+_).collect().foreach(println)

    sc.stop
  }
}
