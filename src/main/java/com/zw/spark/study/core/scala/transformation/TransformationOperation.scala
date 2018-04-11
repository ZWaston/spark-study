package com.zw.spark.study.core.scala.transformation

import org.apache.spark.{SparkConf, SparkContext}

object TransformationOperation {
  def main(args: Array[String]): Unit = {
    //map()
    //filter()
    //flatMap()
    //groupByKey()
    //reduceByKey()
    sortByKey()
    //join()
    //group()
  }
  def map(): Unit = {
    val conf = new SparkConf()
      .setAppName("map_scala")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val numbers = Array(1,2,3,4,5)
    val numbersRDD = sc.parallelize(numbers,2)
    numbersRDD.map(number => number * 2).foreach(num => println(num))
  }

  def filter(): Unit ={
    val conf = new SparkConf()
      .setAppName("filter_scala")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val numbers = Array(1,2,3,4,5,6,7,8,9,10)
    val numbersRDD = sc.parallelize(numbers,1)
    numbersRDD.filter(number => number%2 == 0).foreach(num => println(num))
  }

  def flatMap(): Unit = {
    val conf = new SparkConf()
      .setAppName("flatMap_scala")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val lines = Array("ni hao","hello world")
    val linesRDD = sc.parallelize(lines)
    linesRDD.flatMap(line => line.split(" ")).foreach(word => println(word))
  }

  def groupByKey(): Unit = {
    val conf = new SparkConf()
      .setAppName("groupByKey_scala")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val scoresList = Array(Tuple2("class1", 80), Tuple2("class1", 90), Tuple2("class2", 60))
    val scoresListRDD = sc.parallelize(scoresList, 2)
    scoresListRDD.groupByKey().foreach(score => {
      println("class:" + score._1)
      score._2.foreach(singleScore => println(singleScore));
      println("===================================")
    })
  }

  def reduceByKey(): Unit = {
    val conf = new SparkConf()
      .setAppName("reduceByKey_scala")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val scoresList = Array(
      Tuple2("class1", 80), Tuple2("class1", 90), Tuple2("class2", 60),
      Tuple2("class3", 80), Tuple2("class3", 90), Tuple2("class3", 60),
      Tuple2("class5", 80), Tuple2("class4", 90), Tuple2("class4", 60))
    val scoresListRDD = sc.parallelize(scoresList,5)
    scoresListRDD.foreach(a => println(a._1+": "+a._2))
    //底层reduceByKey实现使用高阶函数combineByKeyWithClassTag
    //只有当同一类key在不同分区时，会将不同分区的同类key，会触发mergeCombiners这个函数
    //因此结果每一类的结果肯定是正确的
    //如这个例子，class1存在于两个分区，但最终将每个分区的结果合并了
    scoresListRDD.reduceByKey(_ + _,3).foreach(classScore => println(classScore._1+": "+classScore._2))
  }

  def sortByKey(): Unit = {
    val conf = new SparkConf()
      .setAppName("sortByKey_scala")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val scoresList = Array(
      Tuple2(80,"A"),Tuple2(80,"A"), Tuple2(50,"C"), Tuple2(100,"B"),
      Tuple2(80,"A"),Tuple2(80,"A"), Tuple2(50,"C"), Tuple2(100,"B"))
    val scoresListRDD = sc.parallelize(scoresList)
    scoresListRDD.foreach(a => println(a._2+": "+a._1))
    //sortByKey（）将每个分区的内部元素进行排序
    scoresListRDD.sortByKey().foreach(studentScore => println(studentScore._2+": "+studentScore._1))
  }

  def join(): Unit ={
    val conf = new SparkConf()
      .setAppName("join_scala")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val studentList = Array(Tuple2(1, "zhangsan"), Tuple2(2, "lisi"), Tuple2(3, "wangwu"),Tuple2(4, "zhaoliu"))
    val studentRDD = sc.parallelize(studentList, 1)
    val scoreList = Array(
      Tuple2(1, 100),
      Tuple2(2, 80),
      Tuple2(3, 60),
      Tuple2(4, 90),
      Tuple2(1, 1001),
      Tuple2(2, 801),
      Tuple2(3, 601),
      Tuple2(4, 901))
    val scoreRDD = sc.parallelize(scoreList,1)

    studentRDD.join(scoreRDD).foreach(studentScore => {
      println("student id: " + studentScore._1)
      println("student name: " + studentScore._2._1)
      println("student score: " + studentScore._2._2)
      println("============================")
    })
  }

  def group(): Unit ={
    val conf = new SparkConf()
      .setAppName("group_scala")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val studentList = Array(Tuple2(1, "zhangsan"), Tuple2(2, "lisi"), Tuple2(3, "wangwu"),Tuple2(4, "zhaoliu"))
    val studentRDD = sc.parallelize(studentList, 1)
    val scoreList = Array(
      Tuple2(1, 100),
      Tuple2(2, 80),
      Tuple2(3, 60),
      Tuple2(4, 90),
      Tuple2(1, 1001),
      Tuple2(2, 801),
      Tuple2(3, 601),
      Tuple2(4, 901))
    val scoreRDD = sc.parallelize(scoreList,1)

    studentRDD.cogroup(scoreRDD).foreach(studentScore => {
      println("student id: " + studentScore._1)
      println("student name: " + studentScore._2._1)
      println("student score: " + studentScore._2._2)
      println("============================")
    })
  }
}
