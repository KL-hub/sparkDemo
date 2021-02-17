package com.lixiang.bigdata.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *  RDD模式匹配
 */
object Spark04_RDD_Operator_Transform3 {
  def main(args: Array[String]): Unit = {
    val oPerator: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OPerator")
    val sc = new SparkContext(oPerator)
    //TODO 算子-map
    val rdd= sc.makeRDD(List(List(1,2),3,List(4,5)))
    val mapRdd: RDD[Any] = rdd.flatMap(data => {
      data match {
        case list: List[_] => list
        case dat => List(dat)
      }
    })
    mapRdd.collect().foreach(println)
    sc.stop()
    }

}
