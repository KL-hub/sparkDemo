package com.lixiang.bigdata.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 *  sample  将数据  求 交集、差集、并集、拉链（可以要求数据类型不一致）
 */
object Spark14_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val oPerator: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OPerator")
    val sc = new SparkContext(oPerator)
    //TODO 算子-map  (Key,Value类型)
    val rdd= sc.makeRDD(List(1,2,3,4))
    val mapRdd: RDD[(Int, Int)] = rdd.map((_, 1))
    mapRdd.partitionBy(new HashPartitioner(2))
      .saveAsTextFile("outpath")
    sc.stop()
    }

}
