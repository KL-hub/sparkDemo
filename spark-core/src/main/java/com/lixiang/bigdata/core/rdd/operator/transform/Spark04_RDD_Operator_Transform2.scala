package com.lixiang.bigdata.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * mapPartitions 可以以分区为单位进行数据转换操作
 *                但是会将整个分区的数据加兹安到内存中进行引用
 *                如果处理完的数据不会被释放掉，存在对象的引用
 *                如内存较小，数据量较大的场合下，容易出现内存溢出
 */
object Spark04_RDD_Operator_Transform2 {
  def main(args: Array[String]): Unit = {
    val oPerator: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OPerator")
    val sc = new SparkContext(oPerator)
    //TODO 算子-map
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))

    val mapRdd: RDD[String] = rdd.flatMap(s => {
      s.split(" ")
    })
    mapRdd.collect().foreach(println)
    sc.stop()
    }

}
