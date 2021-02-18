package com.lixiang.bigdata.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 *
 */
object Spark15_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val oPerator: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OPerator")
    val sc = new SparkContext(oPerator)
    //TODO 算子-map  (Key,Value类型)
    val newRdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)))
    val newRdd2: RDD[(String, Int)] = newRdd.reduceByKey((x: Int, y: Int) => {
      println(s"x=${x},y=${y}")
      x + y
    })
    newRdd2.collect().foreach(println)
    sc.stop()
    }

}
