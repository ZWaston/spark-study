package com.zw.spark.study.core.scala.action

import org.apache.spark.{SparkConf, SparkContext}

object ActionOperation {
  def main(args: Array[String]): Unit = {
      //reduce()
    //collect()
    //count()
    //take()
    countByKey()
  }
  def reduce(): Unit = {
    val conf = new SparkConf().setAppName("reduce_scala").setMaster("local")
    val sc = new SparkContext(conf)
    val numbers = Array(1,2,3,4,5,6,7,8,9,10)
    val numbersRDD = sc.parallelize(numbers,2)
    val sum = numbersRDD.reduce(_ + _)
    println(sum)
  }
  def collect(): Unit ={
    val conf = new SparkConf().setAppName("collect_scala").setMaster("local")
    val sc = new SparkContext(conf)
    val numbers = Array(1,2,3,4,5,6,7,8,9,10)
    val numbersRDD = sc.parallelize(numbers,1)
    val result = numbersRDD.map(_ * 2).collect()
    for(num <- result) {
      println(num)
    }
  }

  def count(): Unit ={
    val conf = new SparkConf().setAppName("count_scala").setMaster("local")
    val sc = new SparkContext(conf)
    val numbers = Array(1,2,3,4,5,6,7,8,9,10)
    val numbersRDD = sc.parallelize(numbers,1)
    val result = numbersRDD.count()
    println(result)
  }

  def take(): Unit ={
    val conf = new SparkConf().setAppName("take_scala").setMaster("local")
    val sc = new SparkContext(conf)
    val numbers = Array(1,2,3,4,5,6,7,8,9,10)
    val numbersRDD = sc.parallelize(numbers,1)
    val result = numbersRDD.take(3)
    for(num <- result) {
      println(num)
    }
  }

  def countByKey(): Unit ={
    val conf = new SparkConf()
      .setAppName("countByKey_scala")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val scoresList = Array(Tuple2("class1", "zhangsan"), Tuple2("class1", "lisi"), Tuple2("class2", "wangwu"))
    val scoresListRDD = sc.parallelize(scoresList, 2)
    val studentCount = scoresListRDD.countByKey()
    for(s <- studentCount) {
      println(s._1+": "+s._2)
    }
  }
}
