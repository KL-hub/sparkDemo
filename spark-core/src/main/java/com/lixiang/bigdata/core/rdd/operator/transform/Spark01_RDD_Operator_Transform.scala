package com.lixiang.bigdata.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val oPerator: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OPerator")
    val sc = new SparkContext(oPerator)
    //TODO 算子-map
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
//    val value: RDD[Int] = rdd.map(_ * 2)
    def mapFunction(num:Int):Int= {
      num *2
    }
    //val mapRdd: RDD[Int] = rdd.map(mapFunction)
    val mapRdd: RDD[Int] = rdd.map((num: Int) => {
      num * 2
    })
    mapRdd.collect().foreach(println)
    sc.stop()
    }

}
