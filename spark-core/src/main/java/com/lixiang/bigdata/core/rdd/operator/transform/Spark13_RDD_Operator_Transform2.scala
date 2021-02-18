package com.lixiang.bigdata.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *  sample  将数据  求 交集、差集、并集、拉链（可以要求数据类型不一致）
 */
object Spark13_RDD_Operator_Transform2 {
  def main(args: Array[String]): Unit = {
    val oPerator: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OPerator")
    val sc = new SparkContext(oPerator)
    //TODO 算子-map
    val rdd= sc.makeRDD(List(1,2,3,4))
    val rdd1= sc.makeRDD(List(3,4,5,6))
    //求交集
    val newRdd: RDD[Int] = rdd.intersection(rdd1)
    println(newRdd.collect().mkString(","))
    //并集
    val newRdd1: RDD[Int] = rdd.union(rdd1)
    println(newRdd1.collect().mkString(","))
    //差集
    val newRdd2: RDD[Int] = rdd.subtract(rdd1)
    println(newRdd2.collect().mkString(","))
    //拉链
    val newRdd3: RDD[(Int, Int)] = rdd.zip(rdd1)
    println(newRdd3.collect().mkString(","))

    sc.stop()
    }

}
