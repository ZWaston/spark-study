package com.zw.spark.study.core.scala

import org.apache.spark.{SparkConf, SparkContext}

object BroadcastVariable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BroadcastVariable").setMaster("local")
    val sc = new SparkContext(conf)
    val factor = 3
    val factorBroadcast = sc.broadcast(factor)

    val numbersList = Array(1,2,3,4,5)
    val numbersRDD = sc.parallelize(numbersList)
    numbersRDD.map(number => number * factorBroadcast.value).foreach(num => println(num))
  }

}
