package com.lixiang.bigdata.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 */
object Spark23_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val oPerator: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OPerator")
    val sc = new SparkContext(oPerator)

    //TODO 算子-map  (Key,Value类型)
    val newRdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2)))
    val newRdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 5), ("b", 6), ("c", 7)))
    //val newRdd3: RDD[(String, (Int, Option[Int]))] = newRdd.leftOuterJoin(newRdd1)
    val newRdd3: RDD[(String, (Iterable[Int], Iterable[Int]))] = newRdd.cogroup(newRdd1)
    newRdd3.collect().foreach(println)
    sc.stop()
    }

}
