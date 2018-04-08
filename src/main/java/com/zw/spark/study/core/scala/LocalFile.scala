package com.zw.spark.study.core.scala

import org.apache.spark.{SparkConf, SparkContext}

object LocalFile {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LocalFile_scala").setMaster("local")
    val sc = new SparkContext(conf)

    val linesRDD = sc.textFile("C:\\Users\\zhang\\Desktop\\test.txt",1)
    val count = linesRDD.map(line => line.length).reduce(_ + _)
    println(count)
  }

}
