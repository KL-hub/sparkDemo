package com.lixiang.bigdata.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 1、RDD的计算一个分区的数据是一个一个执行的逻辑
 *   只有前面一个数据全部的逻辑执行完毕后，才会执行下一个逻辑，分区内数据的执行是 有序的
 */
object Spark01_RDD_Operator_Transform_Par {
  def main(args: Array[String]): Unit = {
    val oPerator: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OPerator")
    val sc = new SparkContext(oPerator)
    //TODO 算子-map
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),1)
    val mapRdd: RDD[Int] = rdd.map(num => {
      println(">>>>>>>>>>>>>" + num)
      num
    })
    val mapRdd1: RDD[Int] = mapRdd.map(num => {
      println("##########" + num)
      num
    })
    mapRdd1.collect()
    //mapRdd.collect().foreach(println)
    sc.stop()
    }

}
