package com.zw.spark.study.core.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * 使用本地文件创建RDD
 * 案例：统计文本文件字数
 */
public class LocalFile {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LocalFile").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //使用textFile（）创建RDD
        JavaRDD<String> lines = sc.textFile("C:\\Users\\zhang\\Desktop\\test.txt",1);
        //统计文本文件内的字数
        JavaRDD<Integer> lineLength = lines.map(new Function<String, Integer>() {
            @Override
            public Integer call(String s) throws Exception {
                return s.length();
            }
        });
        int count = lineLength.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println("文件总字数:" + count);
        sc.close();
    }
}
