package com.zw.spark.study.core.scala.advancedprogramming

import org.apache.spark.{SparkConf, SparkContext}

object SortWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Accumulator").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("C:\\Users\\zhang\\Desktop\\test.txt",1)

    //单词计数
    val wordCounts = lines.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_ + _)

    //key-value反转排序
    val sortedCountWords = wordCounts.map(wordCount => (wordCount._2,wordCount._1)).sortByKey(ascending = false)

    //value-key反转，输出
    sortedCountWords.map(sortedCountWord => (sortedCountWord._2,sortedCountWord._1)).foreach(sortedCountWord => println(sortedCountWord._1 +": " + sortedCountWord._2))
  }

}
