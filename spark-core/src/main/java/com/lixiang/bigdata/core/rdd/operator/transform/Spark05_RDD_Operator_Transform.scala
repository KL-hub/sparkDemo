package com.lixiang.bigdata.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *  glom 将数据  根据分区进行归并
 */
object Spark05_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val oPerator: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OPerator")
    val sc = new SparkContext(oPerator)
    //TODO 算子-map
    val rdd= sc.makeRDD(List(1,2,3,4),2)
    val glomRdd: RDD[Array[Int]] = rdd.glom()
    glomRdd.collect().foreach(data=>println(data.mkString(",")))
    //mapRdd.collect().foreach(println)
    sc.stop()
    }

}
