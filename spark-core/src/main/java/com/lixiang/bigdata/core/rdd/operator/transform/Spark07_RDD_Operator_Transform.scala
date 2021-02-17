package com.lixiang.bigdata.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *  filter  将数据 进行分组
 */
object Spark07_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val oPerator: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OPerator")
    val sc = new SparkContext(oPerator)
    //TODO 算子-map
    val rdd= sc.makeRDD(List(1,2,3,4),2)
    // groupp by 将数据源中的每一个数据进行分组判断，根据返回的分组key进行分组
    val fliterRdd: RDD[Int] = rdd.filter(num => num % 2 == 0)
    fliterRdd.collect().foreach(println)
    sc.stop()
    }

}
