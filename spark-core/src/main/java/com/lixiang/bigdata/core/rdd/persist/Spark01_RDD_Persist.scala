package com.lixiang.bigdata.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 行动算子
 */
object Spark01_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val oPerator: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OPerator")
    val sc = new SparkContext(oPerator)

    val rdd = sc.makeRDD(List("Hello world","Hello Spark"))

    val flatRdd: RDD[String] = rdd.flatMap(_.split(" "))

    val mapRdd: RDD[(String, Int)] = flatRdd.map((_, 1))

    val reduceRdd: RDD[(String, Int)] = mapRdd.reduceByKey(_ + _)

    reduceRdd.collect().foreach(println)

    //cache: 将数据临时存储在内存中 进行数据重用
    reduceRdd.cache()
    //将数据临时 存储在 磁盘文件中进行数据重用，涉及到磁盘IO，性能较低，但是数据安全
    //如果作业执行完毕，临时保存的文件就会消失
    reduceRdd.persist()
    //将数据长久地保存在磁盘文件中进行数据重用，涉及到磁盘IO，性能较低，但是数据安全
    //为了保证数据安全，所以一般情况下，会独立执行作业
    //为了能够提高效率，一般情况下 ，是 需要和cache联合使用
    reduceRdd.checkpoint()

    //TODO
    //所谓的行动算子，其实就是触发job的执行
    rdd.collect();
    sc.stop()
  }
}
