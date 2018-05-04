package com.zw.spark.study.sparkstreaming.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * 实时wordcount程序
 * 编写完之后，打包到集群上本地运行
 * 在服务器上运行nc -lk 9999
 * 那么在这个窗口没输入一行文本就会进行一次统计。
 * 注意的是：每一个批次只会统计该批次的结果，下一次批次不会用到上一次批次的结果
 * 比如：第一个批次统计 “hello world”，第二个批次也统计这个，那么结果都是(hello,1)、(world,1),
 * 而第二个批次的结果不是(hello,2)、(world,2)
 */
public class WordCount {
    public static void main(String[] args) throws InterruptedException {
        //创建SparkConf对象
        //与Spark Core的有一点不同，设置Master属性的时候，使用local模式时，
        // local后面必须跟一个方括号，里面填写一个数字，数字代表了用几个线程执行Spark Streaming程序。
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("WordCountLocal");

        //创建SparkStreamingContext对象
        //该对象除了接收SparkConf对象之外，还必须接收一个batch interval参数，
        // 就是每隔多长时间的数据，划分为一个batch
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(1));

        //首先，创建一个DStream，代表了从一个数据源（如kafka、socket）来的持续不断的实时数据流
        //调用JavaStreamingContext的socketTextStream()方法，可以创建一个数据源为Socket网络端口
        //socketTextStream()方法接收两个参数，第一个参数时哪个主机上的端口，第二个监听哪一个端口
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("localhost", 9999);

        //到这里为止，JavaReceiverInputDStream中的每隔一秒的数据会有一个RDD封装。
        //RDD的元素类型为String，即一行一行的文本
        //所以，这里JavaReceiverInputDStream的泛型<String>代表了底层RDD的泛型类型

        //开始执行计算收到的数据，使用Spark Core提供的算子，执行应用在DStream中即可
        //在底层，实际上就是对DStream的一个个RDD，执行应用在DStream的算子，
        //产生新的RDD，会作为新DStream的RDD
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        //这时一行行的文本被切分成多个单词，words DStream的RDD元素类型为一个个单词
        //接着开始进行mapToPair、reduceByKey操作
        JavaPairDStream<String,Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word,1);
            }
        });

        //和Spark Core很像，只是变成了JavaPairDStream等

        JavaPairDStream<String,Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        //到这里就实现了实时wordcount程序
        //一定要注意，SparkStreaming的计算模型，就决定了，我们必须自己来进行中间缓存的控制，比如写入redis等缓存


        //最后，每次计算完，就打印这一秒钟的单词计数情况
        // Print the first ten elements of each RDD generated in this DStream to the console
        wordCounts.print();



        //首先对JavaStreamingContext进行一下后续处理
        //必须调用JavaStreamingContext的start()方法，整个Java Streaming Application才会启动执行
        //否则，不会执行
        jsc.start();
        try {
            jsc.awaitTermination();//等待应用程序的终止，可以使用CTRL+C手动停止
            //也可以通过调用JavaStreamingContext的stop()方法来终止程序
            //注意点：
            // 1、调用stop()方法后，不能再添加计算逻辑；
            //2、stop()方法会默认停止内部的SparkContext，如果不希望停止，希望
            //还能继续使用SparkContext创建的其他类型的Context，如SQLContext，那么就用stop(false)；
            //3、一个SparkContext可以创建多个StreamingContext，只要对上一个Context停止了，即stop(false)。
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        jsc.close();
    }
}
