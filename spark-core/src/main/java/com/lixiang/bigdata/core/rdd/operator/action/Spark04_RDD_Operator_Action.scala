package com.lixiang.bigdata.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 行动算子
 */
object Spark04_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val oPerator: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OPerator")
    val sc = new SparkContext(oPerator)

    //val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 4, 4),2)
    val rdd1 = sc.makeRDD(List(("a",1),("a",2),("a",3)))

    //TODO
    val stringToLong: collection.Map[String, Long] = rdd1.countByKey()
    println(stringToLong)
    sc.stop()
  }
}
