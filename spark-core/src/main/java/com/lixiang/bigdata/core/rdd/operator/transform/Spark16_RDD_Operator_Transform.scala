package com.lixiang.bigdata.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *groupBy(_._1)  与 groupByKey 区别
 *
 */
object Spark16_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val oPerator: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OPerator")
    val sc = new SparkContext(oPerator)
    //TODO 算子-map  (Key,Value类型)
    val newRdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)))
    val newRdd1: RDD[(String, Iterable[Int])] = newRdd.groupByKey()
    newRdd1.collect().foreach(println)
    //
    newRdd.groupBy(_._1)

    sc.stop()
    }

}
