package com.lixiang.bigdata.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 从文件中创建Rdd
 */
object Spark01_RDD_File {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 创建环境
    //val rdd: RDD[String] = sc.textFile("datas/1.txt")
    val rdd: RDD[(String, String)] = sc.wholeTextFiles("datas/1.txt")
    rdd.collect().foreach(println)
    //TODO 关闭环境
    sc.stop()
  }
}
