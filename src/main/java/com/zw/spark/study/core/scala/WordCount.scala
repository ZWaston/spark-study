package com.zw.spark.study.core.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 提交到集群上的程序，之前报找不到类的错误，后来把compile的scala2.12.5的版本降为2.11.5的，就好了
  * 原因应该是Spark 2.3.0 uses Scala 2.11，最高只支持2.11.x的版本，另外将指定的本地scala给删除，在pom文件指定版本
  * 可以在IDEA查看pom文件各个jar的依赖，红色虚线代表有版本冲突问题
  * ctrl+q 可以快速查看函数的声明
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount_scala").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("hdfs://192.168.223.202:9000/user/zhang/words.txt",1)//
    var words = lines.flatMap(line => line.split(" "))
    val pairs = words.map(word => (word,1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.foreach(wordCount => println(wordCount._1 +":"+ wordCount._2))
  }

}
