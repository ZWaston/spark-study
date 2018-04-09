package com.zw.spark.study.core.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * 本地测试的wordcount程序
 */
public class WordCountLocal {
    public static void main( String[] args ){
        //编写Spark应用程序,在本地运行
        //第一步：创建SparkConf对象，设置Spark应用的配置信息
        //使用setMaster()可以设置Spark应用程序要连接Spark集群的master节点的url
        //但是如果设置为local则代表在本地运行
        SparkConf conf = new SparkConf()
                .setAppName("WordCountLocal")
                .setMaster("local");
        //第二步：创建JavaSparkContext对象
        //在Spark中，SparkContext是spark所有功能的一个入口，无论是用java、scala还是python
        //都必须有一个SparkContext，它的主要作用，包括初始化spark应用程序所需的一些核心组件，包括
        //调度器(DAGSchedule、TaskScheduler),还会去到Spark Master节点上进行注册，等等
        //但是呢，在Spark中，编写不同类型的Spark应用程序，使用的SparkContext是不同的，
            //如果使用的是scala，使用的就是原生的SparkContext对象
            //如果使用的是JAVA，那么就是JavaSparkContext对象
            //如果开发SparkSQL程序，使用SQLContext、HiveContext对象
            //如果使用的是sparkstreaming，那么就是它独有的SparkContext对象
        JavaSparkContext sc = new JavaSparkContext(conf);
        //第三步：针对输入源(HDFS文件、本地文件，等等)，创建一个初始的RDD
        //输入源的数据会被打散，分配到RDD的每个partition，从而形成一个初始的分布式的数据集
        //本例是本地测试，就是针对本地文件
        //SparkContext中，用于根据文件类型的输入源创建RDD的方法，叫做textFile()方法
        //在JAVA中，创建的RDD都叫做JavaRDD
        //在这里，RDD中，有元素这个概念，如果是HDFS或者本地文件，创建的RDD，每一个元素
        //就相当于是文件里的一行
        JavaRDD<String> lines = sc.textFile("C:\\Users\\zhang\\Desktop\\test.txt");

        //第四步：对初始RDD进行transformation操作，计算操作
        //通常会通过创建function，并配合RDD的map、flatMap等算子来执行
        //function，通常，如果比较简单，则创建指定function的匿名内部类

        //现将每一行拆分成单个的单词
        //FlatMapFunction，有两个泛型参数，分别代表输入和输出类型
        //这个算子主要将RDD的一个元素分解为多个元素
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = -6665768067354959668L;

            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        //接着，需要将每一个单词，映射为(单词，1)这种格式
        //因为只有这样，后面才能根据单词作为key，来进行每个单词的出现次数的累加
        //mapToPair，其实就是将每个元素映射为一个(v1,v2)这样Tuple2类型的元素
        //mapToPair这个算子，与PairFunction配合使用
        //第一个泛型参数代表输入类型，第二个和第三个泛型参数，代表的是Tuple2的第一个值和第二个值的类型
        //JavaPairRDD  ,第一个和第二个泛型参数，代表的是Tuple2的第一个值和第二个值的类型
        JavaPairRDD<String,Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = -2986718103822487873L;

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String,Integer>(word,1);
            }
        });

        //接着，需要以单词作为key，统计每个单词出现的次数
        //这里要使用reduceByKey这个算子，并对每个key对于的value，都进行reduce操作
        //JavaPairRDD中有几个元素，分别为(a,1) (b,1) (c,1)
        //相当于就是mareduce的reduce操作，对每个单词进行计数
        //最后返回的是JavaPairRDD中的元素，也是tuple,第一个是key，第二个是key的value
        //reduce之后，相当于每个单词出现的次数
        JavaPairRDD<String,Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 9372238063426138L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });

        //最后，使用action操作，action触发提交任务
        wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            private static final long serialVersionUID = -7341316077192185809L;

            @Override
            public void call(Tuple2<String, Integer> wordcount) throws Exception {
                System.out.println(wordcount._1+": "+wordcount._2);
            }
        });

        sc.close();
    }
}
