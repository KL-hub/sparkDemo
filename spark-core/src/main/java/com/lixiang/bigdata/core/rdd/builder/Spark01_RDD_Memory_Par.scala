package com.lixiang.bigdata.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 从内存中创建Rdd ，并+分区
 */
object Spark01_RDD_Memory_Par {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 创建环境
    //RDD的并行度 & 分区
    //makeRDD 方法可以传递  第二个参数，这个参数表示分区的数量
    //第二个参数可以不传递，那么makeRDD方法会使用默认值
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    rdd.saveAsTextFile("OUTPUT")
    //TODO 关闭环境
    sc.stop()
  }
}
