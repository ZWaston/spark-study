package com.zw.spark.study.core.java.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * transformation操作实战
 */
public class TransformationOperation {
    public static void main(String[] args) {
        //map();
        //filter();
        //flatMap();
        //groupByKey();
        //reduceByKey();
        //sortByKey();
        joinAndGroup();
    }

    /**
     * map算子案例：将每个元素都乘以2
     */
    public static void map() {
        SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //构造集合进行测试
        List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        //并行化集合，创建初始RDD
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
        //使用map算子将每个元素乘以2
        //map算子，对于任何类型的RDD都是可以调用的
        //map()的参数是Function对象，第二个参数是返回类型的参数
        //在call方法内部，对每一个原始RDD的每一个元素进行计算，返回一个新的元素，所有新的元素组成一个新的RDD
        JavaRDD<Integer> mutipleNumberRDD = numberRDD.map(new Function<Integer, Integer>() {
            private static final long serialVersionUID = 2042934655636518158L;

            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });
        //打印新的RDD
        mutipleNumberRDD.foreach(new VoidFunction<Integer>() {
            private static final long serialVersionUID = -9031026659642225067L;

            @Override
            public void call(Integer v1) throws Exception {
                System.out.println(v1);
            }
        });
        sc.close();
    }

    /**
     * filter算子案例，过滤集合中的偶数
     */
    public static void filter() {
        SparkConf conf = new SparkConf().setAppName("filter").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //构造集合进行测试
        List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        //并行化集合，创建初始RDD
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers,2);

        //不想保留的元素，返回false
        JavaRDD<Integer> evenNumberRDD = numberRDD.filter(new Function<Integer, Boolean>() {
            private static final long serialVersionUID = 4843900514094221132L;

            @Override
            public Boolean call(Integer v1) throws Exception {
                return v1 % 2 == 0;
            }
        });
        evenNumberRDD.foreach(new VoidFunction<Integer>() {
            private static final long serialVersionUID = -1352077972289009176L;

            @Override
            public void call(Integer v) throws Exception {
                System.out.println(v);
            }
        });
        sc.close();
    }

    /**
     * 拆分单词
     */
    public static void flatMap() {
        SparkConf conf = new SparkConf().setAppName("flatMap").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> lineList = Arrays.asList("ni hao","hello world","apple is so clever");
        //并行化集合，创建初始RDD
        JavaRDD<String> numberRDD = sc.parallelize(lineList,2);
        //flatMap的返回可以返回多个元素的多个元素，封装在Iterator中
        JavaRDD<String> words = numberRDD.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 3014459035726888289L;

            //这里会，比如，传入ni hao
            //返回的是Iterator<String>(ni,hao)
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        words.foreach(new VoidFunction<String>() {
            private static final long serialVersionUID = -8365100248327025152L;

            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
        sc.close();
    }

    /**
     * groupByKey案例：按照班级成绩进行分组,返回的是JavaPairRDD
     */
    public static void groupByKey() {
        SparkConf conf = new SparkConf().setAppName("groupByKey").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //模拟集合
        List<Tuple2<String,Integer>> scores = Arrays.asList(
                new Tuple2<>("class1",30),
                new Tuple2<>("class2",40),
                new Tuple2<>("class1",50),
                new Tuple2<>("class2",60)
                );
        //并行化集合
        JavaPairRDD<String,Integer> scoresRDD = sc.parallelizePairs(scores);
        //针对scoresRDD，执行groupByKey算子，对每个班级的成绩进行分组
        JavaPairRDD<String,Iterable<Integer> > groupScores = scoresRDD.groupByKey();
        groupScores.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            private static final long serialVersionUID = 57089501856692635L;

            @Override
            public void call(Tuple2<String, Iterable<Integer>> groupScore) throws Exception {
                System.out.println(groupScore._1 + ": " + groupScore._2);
            }
        });
        sc.close();
    }

    /**
     * reduceByKey案例：计算班级总分
     */
    public static void reduceByKey() {
        SparkConf conf = new SparkConf().setAppName("reduceByKey").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //模拟集合
        List<Tuple2<String,Integer>> scores = Arrays.asList(
                new Tuple2<>("class1",30),
                new Tuple2<>("class2",40),
                new Tuple2<>("class1",50),
                new Tuple2<>("class2",60)
        );
        //并行化集合
        JavaPairRDD<String,Integer> scoresRDD = sc.parallelizePairs(scores);
        JavaPairRDD<String,Integer> groupRDD = scoresRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 152563015518527976L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        groupRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            private static final long serialVersionUID = -6275192572350143331L;

            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1 +": " + t._2);
            }
        });
        sc.close();
    }
    /**
     * sortByKey:将学生分数进行排序
     */
    public static void sortByKey() {
        SparkConf conf = new SparkConf().setAppName("sortByKey").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //模拟集合
        List<Tuple2<Integer,String>> scores = Arrays.asList(
                new Tuple2<>(50,"A"),
                new Tuple2<>(60,"B"),
                new Tuple2<>(20,"C"),
                new Tuple2<>(100,"D")
        );
        //并行化集合
        JavaPairRDD<Integer,String> scoresRDD = sc.parallelizePairs(scores);

        JavaPairRDD<Integer,String> sortedRDD = scoresRDD.sortByKey(false);
        sortedRDD.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            private static final long serialVersionUID = -914561644513001192L;

            @Override
            public void call(Tuple2<Integer, String> t) throws Exception {
                System.out.println(t._2 +": " + t._1);
            }
        });
        sc.close();
    }

    /**
     * join和cogroup案例：打印每个学生的成绩
     * 这两个区别就是返回类型不同
     * cogroup,与join不同，相当于是一个key join上的所有value，都给放到一个Iterable里面去了
     */
    private static void joinAndGroup() {
        SparkConf conf = new SparkConf().setAppName("joinAndGroup").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //模拟集合
        List<Tuple2<Integer,String>> studentList = Arrays.asList(
                new Tuple2<>(1,"zhangsan"),
                new Tuple2<>(2,"lisi"),
                new Tuple2<>(3,"wangwu"),
                new Tuple2<>(4,"zhaoliu")
        );
        List<Tuple2<Integer,Integer>> scoreList = Arrays.asList(
                new Tuple2<>(1,100),
                new Tuple2<>(2,80),
                new Tuple2<>(3,60),
                new Tuple2<>(4,90),
                new Tuple2<>(1,1001),
                new Tuple2<>(2,801),
                new Tuple2<>(3,601),
                new Tuple2<>(4,901)
        );
        //并行化两个RDD
        JavaPairRDD<Integer,String> studentRDD = sc.parallelizePairs(studentList);
        JavaPairRDD<Integer,Integer> scoreRDD = sc.parallelizePairs(scoreList);


        //使用join算子关联两个RDD,join是根据key进行join操作，RDD的第一个是key，第二个是value
        //并返回一个JavaPairRDD对象，第一个泛型类型是key的类型
        //第二个泛型类型Tuple2<v1,v2>分别是原始RDD的value的类型
        JavaPairRDD<Integer,Tuple2<String,Integer>> studentScoreRDD = studentRDD.join(scoreRDD);

        //打印studentScore
        studentScoreRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            private static final long serialVersionUID = -3715232392423835566L;

            @Override
            public void call(Tuple2<Integer, Tuple2<String, Integer>> t) throws Exception {
                System.out.println("student id: " + t._1);
                System.out.println("student name: " + t._2._1);
                System.out.println("student score: " + t._2._2);
                System.out.println("============================" );
            }
        });


        /*
        //使用cogroup,与join不同，相当于是一个key join上的所有value，都给放到一个Iterable里面去了
        JavaPairRDD<Integer,Tuple2<Iterable<String>,Iterable<Integer>>> studentScoreRDD = studentRDD.cogroup(scoreRDD);
        studentScoreRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t) throws Exception {
                System.out.println("student id: " + t._1);
                System.out.println("student name: " + t._2._1);
                System.out.println("student score: " + t._2._2);
                System.out.println("============================" );
            }
        });*/
        sc.close();
    }
}
