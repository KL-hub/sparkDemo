package com.lixiang.bigdata.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *  glom 将数据  根据分区进行归并
 *  将同一个分区的数据直接转换为相同类型的内存数组进行处理，分区不变
 */
object Spark05_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val oPerator: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OPerator")
    val sc = new SparkContext(oPerator)
    //TODO 算子-map
    val rdd= sc.makeRDD(List(1,2,3,4),2)
    val glomRdd: RDD[Array[Int]] = rdd.glom()
    val maxRdd: RDD[Int] = glomRdd.map(array => {
      array.max
    })
    println(maxRdd.collect().sum)
    //mapRdd.collect().foreach(println)
    sc.stop()
    }

}
