package com.zw.spark.study.core.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * 使用HDFS文件创建RDD,打包到集群上运行
 * 案例：统计文本文件字数
 */
public class HDFSFile {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("HDFSFile");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //使用textFile（）创建RDD,针对HDFS文件，改成HDFS的路径
        JavaRDD<String> lines = sc.textFile("hdfs://192.168.223.202:9000/user/zhang/words.txt",1);
        //统计文本文件内的字数
        JavaRDD<Integer> lineLength = lines.map(new Function<String, Integer>() {
            private static final long serialVersionUID = -7830118247303761017L;

            @Override
            public Integer call(String s) throws Exception {
                return s.length();
            }
        });
        int count = lineLength.reduce(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 778785231609983779L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println("文件总字数:" + count);
        sc.close();
    }

}
