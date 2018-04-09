package com.zw.spark.study.core.scala

import org.apache.spark.{SparkConf, SparkContext}

object Accumulator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Accumulator").setMaster("local")
    val sc = new SparkContext(conf)

    val sum = sc.accumulator(0)

    val numbersList = Array(1,2,3,4,5)
    val numbersRDD = sc.parallelize(numbersList)

    numbersRDD.foreach(num => sum += num)

    println(sum)
  }

}
