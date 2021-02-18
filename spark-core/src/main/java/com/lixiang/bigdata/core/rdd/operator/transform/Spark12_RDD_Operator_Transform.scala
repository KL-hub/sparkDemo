package com.lixiang.bigdata.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *  sample  将数据 抽取部分样本
 */
object Spark12_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val oPerator: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OPerator")
    val sc = new SparkContext(oPerator)
    //TODO 算子-map
    val rdd= sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10),2)
    val newRdd: RDD[Int] = rdd.sortBy(num => num, false)
    newRdd.collect().foreach(println)
    sc.stop()
    }

}
