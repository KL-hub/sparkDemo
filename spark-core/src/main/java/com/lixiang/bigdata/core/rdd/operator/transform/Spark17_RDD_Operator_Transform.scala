package com.lixiang.bigdata.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *groupBy(_._1)  与 groupByKey 区别
 *
 */
object Spark17_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val oPerator: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OPerator")
    val sc = new SparkContext(oPerator)
    //TODO 算子-map  (Key,Value类型)
    val newRdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)),2)
     //第一个参数  初始值 主要用于碰见第一个key时，和分区内做计算
    //aggregateByKey第二个参数需要传递2个参数
       //第一个参数表示分区内计算规则
       //第二个参数表示分区见计算规则
    val newRdd2: RDD[(String, Int)] = newRdd.aggregateByKey(0)((x, y) => {
      math.max(x, y)
    },(x,y)=>{
      x+y
    })
    newRdd2.collect().foreach(println)
    sc.stop()
    }

}
