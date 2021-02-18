package com.lixiang.bigdata.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *  sample  将数据 排序
 */
object Spark12_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val oPerator: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OPerator")
    val sc = new SparkContext(oPerator)
    //TODO 算子-map
    val rdd= sc.makeRDD(List(("1",1),("11",2),("2",3)),2)
    val newRdd: RDD[(String, Int)] = rdd.sortBy(t => t._1, false)
    newRdd.collect().foreach(println)
    sc.stop()
    }

}
