package com.zw.spark.study.core.scala

import org.apache.spark.{SparkConf, SparkContext}

object ParallelizeCollection {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ParallelizeCollection_scala").setMaster("local")
    val sc = new SparkContext(conf)

    val numbers = Array(1,2,3,4,5,6,7,8,9,10)
    val numbersRDD = sc.parallelize(numbers,5)
    val sum = numbersRDD.reduce(_ + _)
    println(sum)
  }

}
