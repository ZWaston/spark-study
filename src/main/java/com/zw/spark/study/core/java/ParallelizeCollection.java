package com.zw.spark.study.core.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * 并行化集合创建RDD
 */
public class ParallelizeCollection
{
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ParallelizeCollection").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //要通过并行化集合的方式创建RDD，那么就需要调用SparkContext以及其子类的parallelize()方法
        List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers,5);

        //执行reduce算子操作
        //相当于，先执行1+2=3，然后3+3=6，然后6+4=10，...
        int sum = numberRDD.reduce(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = -8751100891405603614L;

            @Override
            public Integer call(Integer num1, Integer num2) throws Exception {
                return num1+num2;
            }
        });
        //输出累加的和
        System.out.println(sum);
        //关闭JavaSparkContext
        sc.close();

    }
}
