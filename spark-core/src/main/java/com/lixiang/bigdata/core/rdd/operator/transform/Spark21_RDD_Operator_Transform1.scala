package com.lixiang.bigdata.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 */
object Spark21_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val oPerator: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OPerator")
    val sc = new SparkContext(oPerator)

    //join 两个不同数据源的数据，相同的key的value会连接在一起，形成元祖
    //     如果两个数据源中key没有匹配上，那么数据不会出现在 结果中
    //     如果两个数据源中key有多个，则做笛卡尔积
    //TODO 算子-map  (Key,Value类型)
    val newRdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("d", 2), ("c", 3), ("d", 4)))
    val newRdd1: RDD[(String, Int)] = sc.makeRDD(List(("d", 5), ("b", 6), ("c", 7), ("d", 8)))
    val newRdd3: RDD[(String, (Int, Int))] = newRdd.join(newRdd1)
    newRdd3.collect().foreach(println)
    sc.stop()
    }

}
