package com.lixiang.bigdata.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *groupBy(_._1)  与 groupByKey 区别
 *
 */
object Spark17_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val oPerator: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OPerator")
    val sc = new SparkContext(oPerator)
    //TODO 算子-map  (Key,Value类型)
    val newRdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)),2)
     //第一个参数  初始值 主要用于碰见第一个key时，和分区内做计算
    //aggregateByKey第二个参数需要传递2个参数
       //第一个参数表示分区内计算规则
       //第二个参数表示分区见计算规则
    newRdd.aggregateByKey(0)(_+_,_+_).collect().foreach(println)

    //如果 聚合计算时，分区内和分区间计算规则相同，spark提供了简化的方法
    newRdd.foldByKey(0)(_+_).collect().foreach(println)
    sc.stop()
    }

}
