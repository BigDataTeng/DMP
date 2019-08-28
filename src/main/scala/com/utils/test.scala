package com.utils

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 测试jason
  */
object test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    //116.350892,40.067575  || 116.481488,39.990464
    val list= List("116.350892,40.067575")
    val rdd = sc.makeRDD(list)
    val bs = rdd.map(t=> {
      val arr: Array[String] = t.split(",")
      AmapUtil.getBusinessFromAmap(arr(0).toDouble,arr(1).toDouble)
    })
    bs.foreach(println)
  }
}
