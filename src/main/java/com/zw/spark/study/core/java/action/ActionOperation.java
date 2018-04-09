package com.zw.spark.study.core.java.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * action操作实战
 */
public class ActionOperation {
    public static void main(String[] args) {
        //reduce();
        //collect();
        //count();
        //take();
        countByKey();
    }
    private static void reduce() {
        SparkConf conf = new SparkConf().setAppName("reduce").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //对1-10的集合数字累加
        List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers,1);

        //执行reduce算子操作
        //相当于，先执行1+2=3，然后3+3=6，然后6+4=10，...
        int sum = numberRDD.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer num1, Integer num2) throws Exception {
                return num1+num2;
            }
        });
        //输出累加的和
        System.out.println(sum);
        sc.close();
    }
    private static void collect() {
        SparkConf conf = new SparkConf().setAppName("collect").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers,1);
        JavaRDD<Integer> multipleRDD = numberRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v) throws Exception {
                return v * 2;
            }
        });
        //不用foreach action操作，在远程集群上遍历RDD中的元素
        //而使用action操作，将结果返回到Driver程序中
        //一般不建议使用，涉及到大量网络传输和可能会造成内存溢出
        List<Integer> result = multipleRDD.collect();
        for(Integer num : result) {
            System.out.println(num);
        }
        sc.close();
    }

    private static void count() {
        SparkConf conf = new SparkConf().setAppName("count").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers,1);
        //对RDD使用count操作，统计有多少个元素
        long num = numberRDD.count();
        System.out.println(num);
        sc.close();
    }

    private static void take() {
        SparkConf conf = new SparkConf().setAppName("take").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers,1);
        //对RDD使用take(n)操作，与collect类似，但他是获取前n个元素
        List<Integer> nums = numberRDD.take(3);
        for(Integer num:nums) {
            System.out.println(num);
        }
        sc.close();
    }

    private static void countByKey() {
        SparkConf conf = new SparkConf().setAppName("countByKey").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //模拟集合
        List<Tuple2<String,String>> scores = Arrays.asList(
                new Tuple2<>("class1","zhangsan"),
                new Tuple2<>("class2","lisi"),
                new Tuple2<>("class1","wangwu"),
                new Tuple2<>("class2","zhaoliu")
        );
        //并行化集合
        JavaPairRDD<String,String> scoresRDD = sc.parallelizePairs(scores);
        //对RDD应用countByKey操作，统计每个班级学生人数，也就是统计每个key对于的元素个数
        Map<String,Long> studentCount = scoresRDD.countByKey();
        for(Map.Entry<String,Long> scount:studentCount.entrySet()) {
            System.out.println(scount.getKey()+": "+scount.getValue());
        }
    }
}
