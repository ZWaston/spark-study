package com.zw.spark.study.core.scala

import org.apache.spark.{SparkConf, SparkContext}

object LineCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LineCount_scala").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("C:\\Users\\zhang\\Desktop\\test.txt",3)
    lines.map(line => (line,1)).reduceByKey(_ + _).foreach(lineCount => println(lineCount._1 + ": " +lineCount._2) )

  }

}
