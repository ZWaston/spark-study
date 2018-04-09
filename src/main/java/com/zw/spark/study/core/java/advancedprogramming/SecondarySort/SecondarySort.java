package com.zw.spark.study.core.java.advancedprogramming.SecondarySort;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * 二次排序
 *  1 5
 *  2 4
 *  3 6
 *  1 3
 *  2 1
 *  对上述进行排序，先按照第一列进行排序，若第一列相同的按照第二列进行排序
 *  1、实现自定义的key，要实现Ordered接口和Serializable接口，在key中实现自己对多个列的排序算法
 *  2、将包含文本的RDD映射成key为自定义key，value为文本的JavaPairRDD
 *  3、使用sortByKey算子按照自定义的key进行排序
 *  4、再次映射，剔除自定义的key，保留文本行
 */
public class SecondarySort {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("SecondarySort")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("C:\\Users\\zhang\\Desktop\\sort.txt",1);
        JavaPairRDD<SecondarySortKey,String> pairs = lines.mapToPair(new PairFunction<String, SecondarySortKey, String>() {
            private static final long serialVersionUID = -7724414351192847430L;

            @Override
            public Tuple2<SecondarySortKey, String> call(String line) throws Exception {
                String[] lineSpilted = line.split(" ");
                SecondarySortKey key = new SecondarySortKey(
                        Integer.valueOf(lineSpilted[0]),
                        Integer.valueOf(lineSpilted[1]));
                return new Tuple2<SecondarySortKey, String>(key,line);
            }
        });

        JavaPairRDD<SecondarySortKey, String> sortedPairs = pairs.sortByKey();
        JavaRDD<String> sortedLines = sortedPairs.map(new Function<Tuple2<SecondarySortKey, String>, String>() {
            private static final long serialVersionUID = -5694371948885765345L;

            @Override
            public String call(Tuple2<SecondarySortKey, String> v) throws Exception {
                return v._2;
            }
        });

        sortedLines.foreach(new VoidFunction<String>() {
            private static final long serialVersionUID = -8129637922616809157L;

            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        sc.close();
    }
}
