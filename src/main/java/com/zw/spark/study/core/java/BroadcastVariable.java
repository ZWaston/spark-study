package com.zw.spark.study.core.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

/**
 * 广播变量
 */
public class BroadcastVariable {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("BroadcastVariable")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        final int factor = 3;//不使用共享变量
        //在JAVA中，使用JavaSparkContext的broadcast()将变量转换为广播变量
        //注意：广播变量是只读的
        final Broadcast<Integer> factorBroadcast = sc.broadcast(factor);
        List<Integer> numbersList = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> numbersRDD = sc.parallelize(numbersList);
        JavaRDD<Integer> numberRDD = numbersRDD.map(new Function<Integer, Integer>() {

            @Override
            public Integer call(Integer v1) throws Exception {
                //使用广播变量，调用value方法获取内部的值
                int factor = factorBroadcast.value();
                return v1 * factor;
            }
        });
        numberRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer v) throws Exception {
                System.out.println(v);
            }
        });
        sc.close();
    }
}
