package com.zw.spark.study.core.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

/**
 * RDD持久化
 * 持久化级别选择建议：
 * 1、优先使用MEMORY_ONLY，纯内存最快且没有CPU消耗
 * 2、MEMORY_ONLY无法存储所有数据时，使用MEMORY_ONLY_SER，只是需要消耗CPU
 * 3、如果需要快速的失败恢复，使用带_2的策略
 * 4、尽量避免使用DISK相关策略，从磁盘读取，有时还不如重新计算
 */
public class Persist {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Persist")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //选择是否对RDD进行持久化操作,当数据量较大时，效果较明显
        //cache()或者persist()的使用是有规则的
        //必须在transformation或者textFile等创建了一个RDD之后，直接连续调用cache()或者persist()才可以
        //如果先创建一个RDD，单另起一行cache()或者persist()，会报错
        //persist()有多种持久化策略，MEMORY_AND_DISK、MEMORY_ONLY、DISK_ONLY、MEMORY_ONLY_SER、MEMORY_AND_DISK_SER、MEMORY_ONLY_2、MEMORY_AND_DISK_2等
        //MEMORY_ONLY：以非序列化的Java对象的方式持久化在JVM内存中。如果内存不够，那么在下一次需要时，没有被持久化的Partition会被重新计算
        //MEMORY_ONLY_SER：同MEMORY_ONLY，但是会使用Java序列化方式，将Java对象序列化后进行持久化。可以减少内存开销，但是要进行反序列化，因此会增加CPU开销
        //如果尾部加了2的持久化级别，表示会将持久化数据复用一份，保存到其他节点，从而在数据丢失时，不需要重新计算，只需要使用备份数据即可
        JavaRDD<String> lines = sc.textFile("C:\\Users\\zhang\\Desktop\\test.txt").cache();
        long beginTime = System.currentTimeMillis();
        long count = lines.count();
        System.out.println(count);
        long endTime = System.currentTimeMillis();
        System.out.println("cost: " + (endTime - beginTime) + "ms.");

        //再做一次count操作
        beginTime = System.currentTimeMillis();
        count = lines.count();
        System.out.println(count);
        endTime = System.currentTimeMillis();
        System.out.println("cost: " + (endTime - beginTime) + "ms.");
        sc.close();
    }
}
