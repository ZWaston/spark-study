package com.zw.spark.study.core.scala.advancedprogramming

import org.apache.spark.{SparkConf, SparkContext}

object Top3 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Top3").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("C:\\Users\\zhang\\Desktop\\top.txt",1)

    val pairs = lines.map(line => (line.toInt,line))
    val sortedPairs = pairs.sortByKey(false)
    val sortedNumbersList = sortedPairs.map(sortedPair => sortedPair._2).take(3)
    sortedNumbersList.foreach(sortedNumber => println(sortedNumber))
  }

}
