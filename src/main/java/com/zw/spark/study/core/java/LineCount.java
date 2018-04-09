package com.zw.spark.study.core.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * 统计每行出现的次数，使用key-value的算子
 */
public class LineCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LineCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //创建初始RDD
        JavaRDD<String> lines = sc.textFile("C:\\Users\\zhang\\Desktop\\test.txt");
        //对line RDD执行mapToPair算子，将每一行映射为(line,1)的这种key-value的格式
        //然后后面才能统计每一行出现的次数
        JavaPairRDD<String,Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = -4990833511942886892L;

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String,Integer>(s,1);
            }
        });

        //使用reduceBy算子，统计每一行出现的总次数
        JavaPairRDD<String,Integer> lineCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 7867306033125271589L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        //执行一个action操作
        lineCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            private static final long serialVersionUID = -6131153853189025632L;

            @Override
            public void call(Tuple2<String, Integer> lineCount) throws Exception {
                System.out.println(lineCount._1 + ": " + lineCount._2);
            }
        });
        sc.close();
    }
}
