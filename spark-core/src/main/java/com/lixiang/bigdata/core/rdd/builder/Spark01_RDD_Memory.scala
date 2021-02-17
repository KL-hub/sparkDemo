package com.lixiang.bigdata.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 从内存中创建Rdd
 */
object Spark01_RDD_Memory {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 创建环境
    //从内存中创建RDD,将内存中集合的数据作为处理的数据源
    val seq: Seq[Int] = Seq[Int](1, 2, 3, 4)
    val rdd: RDD[Int] = sc.parallelize(seq)
    rdd.collect().foreach(println)
    //TODO 关闭环境
    sc.stop()
  }
}
