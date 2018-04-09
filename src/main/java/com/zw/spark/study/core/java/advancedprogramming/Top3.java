package com.zw.spark.study.core.java.advancedprogramming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.List;

/**
 * 取最大的前3个数字
 */
public class Top3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Top3")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("C:\\Users\\zhang\\Desktop\\top.txt",1);
        JavaPairRDD<Integer,String> numbers = lines.mapToPair(new PairFunction<String, Integer,String>() {
            private static final long serialVersionUID = -2737516428912032074L;

            @Override
            public Tuple2<Integer,String> call(String s) throws Exception {
                return new Tuple2<>(Integer.valueOf(s),s);
            }
        });

        JavaPairRDD<Integer,String> sortedPairs = numbers.sortByKey(false);

        JavaRDD<String> sortedNumbers = sortedPairs.map(new Function<Tuple2<Integer, String>, String>() {
            @Override
            public String call(Tuple2<Integer, String> t) throws Exception {
                return t._2;
            }
        });

        List<String> sortedNumbersList =  sortedNumbers.take(3);
        for(String s:sortedNumbersList) {
            System.out.println(s);
        }
        sc.close();
    }
}
