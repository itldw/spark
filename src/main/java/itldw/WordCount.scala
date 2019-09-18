package itldw

import org.apache.spark._

object WordCount {

  def main(args: Array[String]): Unit = {

    val wordFile = "file:///usr/local/spark234/REDEME.MD"
    val conf = new SparkConf().setAppName("wordcount");
    val sc = new SparkContext(conf)
    val input = sc.textFile(wordFile, 2).cache()
    val lines = input.flatMap(line=>line.split(" "))
    val count = lines.map(word => (word,1)).reduceByKey{case (x,y)=>x+y}
    val output = count.saveAsTextFile("/home/hadoop/hellospark")


  }
}
