package com.lixiang.bigdata.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 行动算子
 */
object Spark03_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val oPerator: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OPerator")
    val sc = new SparkContext(oPerator)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)

    //TODO
    //所谓的行动算子，其实就是触发job的执行

    //aggregate 初始值 不能执行分区内计算，还执行分区间计算
    val i: Int = rdd.aggregate(10)(_ + _, _ + _)
    //fold 分区内与分区间相同时，可用fold，效果与aggregate相同，只不过少一个参数
    val i1: Int = rdd.fold(10)(_ + _)
    println(i)
    println(i1)
    sc.stop()
  }
}
