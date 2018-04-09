package com.zw.spark.study.core.java.advancedprogramming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 分组取Top3
 * 示例数据：
 class1 40
 class1 50
 class1 70
 class1 90
 class2 100
 class2 80
 class2 90
 class2 95
 */
public class GroupTop3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("GroupTop3")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("C:\\Users\\zhang\\Desktop\\score.txt",1);

        JavaPairRDD<String,Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = -4136754394683612751L;

            @Override
            public Tuple2<String, Integer> call(String t) throws Exception {
                return new Tuple2<String, Integer>(t.split(" ")[0],
                        Integer.valueOf(t.split(" ")[1]));
            }
        });

        //进行按班级分组
        JavaPairRDD<String,Iterable<Integer>> groupedPairs = pairs.groupByKey();

        JavaPairRDD<String,Iterable<Integer>> top3Score = groupedPairs.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
            private static final long serialVersionUID = 8132245109521762273L;

            @Override
            public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> classScores) throws Exception {
                Integer[] top3 = new Integer[3];
                String className = classScores._1;
                Iterator<Integer> scores = classScores._2.iterator();
                //获得最大的三个数
                while(scores.hasNext()) {
                    Integer score = scores.next();

                    for(int i = 0; i < 3; i++) {
                        if(top3[i] == null) {
                            top3[i] = score;
                            break;
                        }else if(score > top3[i]) {
                            for(int j = 2; j > i; j--) {
                                top3[j] = top3[j - 1];
                            }
                            top3[i] = score;
                            break;
                        }
                    }
                }

                return new Tuple2<String, Iterable<Integer>>(className, Arrays.asList(top3));
            }
        });

        top3Score.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            private static final long serialVersionUID = 8088388745656332638L;

            @Override
            public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                System.out.println("class: " + t._1);
                Iterator<Integer> scoreIterator = t._2.iterator();
                while(scoreIterator.hasNext()) {
                    Integer score = scoreIterator.next();
                    System.out.println(score);
                }
                System.out.println("====================");
            }
        });
    }
}
