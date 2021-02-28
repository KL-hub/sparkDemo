package com.lixiang.bigdata.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 行动算子
 */
object Spark01_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val oPerator: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OPerator")
    val sc = new SparkContext(oPerator)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //TODO
    //所谓的行动算子，其实就是触发job的执行
    rdd.collect();
    sc.stop()
  }
}
