package com.zw.spark.study.core.java.advancedprogramming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * 排序的wordcount程序
 */
public class SortWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("SortWordCount")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("C:\\Users\\zhang\\Desktop\\test.txt",1);
        //执行单词计数
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        JavaPairRDD<String,Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String,Integer>(word,1);
            }
        });
        JavaPairRDD<String,Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
        //到这里就得到了每个单词的数量
        //但是新需求是按照每个单词出现次数的顺序进行降序排序
        //wordCounts的元素是什么？格式为:(hello,3)、(world,2)
        //则需要将该元素转变成(3,hello)、(4,world)的格式才能进行排序

        //进行key、value的反转映射
        JavaPairRDD<Integer,String> countWords = wordCounts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
                return new Tuple2<Integer, String>(t._2,t._1);
            }
        });
        //按照key进行排序
        JavaPairRDD<Integer,String> sortedCountWords = countWords.sortByKey(false);

        //再次将value-key反转映射
        JavaPairRDD<String, Integer> sortedWordCounts = sortedCountWords.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
                return new Tuple2<String, Integer>(t._2,t._1);
            }
        });

        //目前已获得排序后的单词计数
        sortedWordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1 + ": " + t._2);
            }
        });

        sc.close();
    }
}
