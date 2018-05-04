package com.zw.spark.study.sparkstreaming.scala

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("LocalWordCount_scala")

    //Scala中，创建的是StreamingContext，设置为5s，便于观察
    val ssc = new StreamingContext(conf,Seconds(5))

    val lines = ssc.socketTextStream("localhost",9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word,1))
    val wordcount = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordcount.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
