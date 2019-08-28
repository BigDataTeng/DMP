package com.Tags

import com.utils.{JedisPool, TagUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 上下文标签
  */
object TagsContext2 {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("目录不匹配，退出程序")
      sys.exit()
    }
    val Array(inputPath, outputPath) = args
    //创建上下文
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    //读取数据
    val df = sQLContext.read.parquet(inputPath)
    //获取停用词
    val stopwords = sc.textFile("F:\\Big-Data-22\\项目\\资料\\Spark用户画像分析\\stopwords.txt").map((_,1)).collectAsMap()
    //广播停用词
    val bcstopword: Broadcast[collection.Map[String, Int]] = sc.broadcast(stopwords)

    //过滤符合Id的数据
    val rdd = df.filter(TagUtils.OneUserId).rdd
      .mapPartitions(row=>{
        //接下来所有的标签都在内部实现
        val jedis = JedisPool.getConnections()
        var list = List[(String,List[(String,Int)])]()
        row.map(row => {
          //取出用户Id
          val userId = TagUtils.getOneUserId(row)

          //广告位类型标签
          val adList = TagsAd.makeTags(row)

          //App名称标签
          val AppList = TagsApp.makeTags(row,jedis)

          //渠道标签
          val adplatformproviderList = TagsAdplatformprovider.makeTags(row)

          //设备标签
          val deviceList = TagDevice.makeTags(row)

          //关键字标签
          val keyWordList = TagKeyWord.makeTags(row,bcstopword)

          //地域标签
          val proCityList = TagProCity.makeTags(row)

          //上下文标签
          list:+=(userId,adList,AppList,adplatformproviderList,deviceList,keyWordList,proCityList)
        })
        JedisPool.close(jedis)
        list.iterator
      })
    val res = rdd.reduceByKey((list1, list2) =>
      // List(("lN插屏",1),("LN全屏",1),("ZC沈阳",1),("ZP河北",1)....)
    {
      (list1 ::: list2)
        .groupBy(_._1) //String,List(String,Int)
        .mapValues(
        x=>{  //List(String,Int)
          x.foldLeft[Int](0)(
            (x,y)=>{x+y._2})
        })     //Map[String, Int]
        .toList
//      (list1:::list2)
//        // List(("APP爱奇艺",List()))
//        .groupBy(_._1)
//        .mapValues(_.foldLeft[Int](0)(_+_._2))
//        .toList
    }
    )
    res.foreach(println)

  }
}
