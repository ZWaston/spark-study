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
 * wordcount程序部署到集群
 *
 * /脚本编写，使用bin/spark-submit脚本提交作业.standalone模式
 * opt/spark/spark-2.2.1-bin-hadoop2.6/bin/spark-submit \
 --class com.zw.spark.study.core.java.WordCountCluster \
 --driver-cores 1 \
 --driver-memory 1g \
 --executor-memory 768m \
 --executor-cores 1 \
 /home/zhangwangcheng/sparktest/java/spark.study-1.0-SNAPSHOT.jar \
 --master spark://ubuntu1:7077
 如果不加master选项，会默认在本地运行
 */
public class WordCountCluster {
    public static void main( String[] args ){

        //更改两个地方
        //第一，将SparkConf的setMaster删掉，默认会自己连接
        //第二，针对的不是本地文件，改成hdfs上的大文件
        //1、使用maven命令打包到集群上clean package
        //2、编写spark-submit脚本
        //3、执行spark-submit脚本，提交spark应用到集群执行
        SparkConf conf = new SparkConf()
                .setAppName("WordCountCluster");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("hdfs://192.168.223.202:9000/user/zhang/words.txt");
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = -3955642046190091252L;

            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        JavaPairRDD<String,Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = -5118561855396064804L;

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String,Integer>(word,1);
            }
        });

        JavaPairRDD<String,Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = -5485475717837989565L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });

        /*
        wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> wordcount) throws Exception {
                System.out.println(wordcount._1+": "+wordcount._2);
            }
        });*/
        //将结果输出到hdfs文件中
        wordCounts.saveAsTextFile("/wordcountresult");

        sc.close();
    }
}
