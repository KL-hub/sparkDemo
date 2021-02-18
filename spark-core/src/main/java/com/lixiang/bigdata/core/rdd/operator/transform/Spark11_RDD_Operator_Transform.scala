package com.lixiang.bigdata.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *  sample  将数据 抽取部分样本
 */
object Spark11_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val oPerator: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OPerator")
    val sc = new SparkContext(oPerator)
    //TODO 算子-map
    val rdd= sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10),2)
    //coalesce 算子可以扩大分区，但是如果不进行shuffle操作，是没有意义的，不起作用
    //所以 如果想要实现 扩大分区的效果，需要使用 shuffle
    // spark提供了一个简化的操作，如果想要数据均衡，可以采用  shuffle
    //repartition
    //val newRdd: RDD[Int] = rdd.coalesce(3, true)
    val newRdd: RDD[Int] = rdd.repartition(3)
    newRdd.saveAsTextFile("outpath")
    sc.stop()
    }

}
