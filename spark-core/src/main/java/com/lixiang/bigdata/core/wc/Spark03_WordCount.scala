package com.lixiang.bigdata.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_WordCount {
  def main(args: Array[String]): Unit = {
    // appliation 调用  spark框架？？
    //TODO 简历与spark的链接
    val sparkConf =new SparkConf().setMaster("local").setAppName("WordCount")
    val sc=new SparkContext(sparkConf)
    //TODO 执行业务操作
    //1、读取文件，获取一行一行的数据
    val lines:RDD[String]=sc.textFile("datas/1.txt")
    //2、将一行数据进行拆分，形成一个一个的单词
    val words:RDD[String]=lines.flatMap(_.split(" "))

    val wordToOne=words.map(word=>(word,1))
    //Spark 框架提供了更多的功能，可以将分组和聚合使用一个方法实现
    var wordToCount=wordToOne.reduceByKey(_+_)
    //5、将转换结果采集到控制台打印出来
    val array:Array[(String,Int)]= wordToCount.collect()
    array.foreach(println)
    //TODO 关闭链接
    sc.stop()
  }
}
