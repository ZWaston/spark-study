package com.zw.spark.study.core.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * 累加变量，主要用于多个节点对一个变量进行共享性的操作，Accumulator只提供了累加的功能。
 * 提供了多个task对一个变量进行并行操作的功能，但是task只能对Accumulator进行累加，不能读取它的值，
 * 只有Driver程序可以读取Accumulator的值
 */
public class Accumulator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Accumulator")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //创建Accumulator变量需要调用accumulator()方法
        org.apache.spark.Accumulator<Integer> sum = sc.accumulator(0);
        List<Integer> numbersList = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> numbersRDD = sc.parallelize(numbersList);
        numbersRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer t) throws Exception {
                //函数内部调用add()方法,就可以对accumulator变量累加
                sum.add(t);
            }
        });
        //在Driver程序中，可以调用accumulator的value()方法，获取其值
        System.out.println(sum.value());
        sc.close();
    }
}
