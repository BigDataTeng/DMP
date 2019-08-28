package com.Tags

import com.Tags.TagsEquipment.{TagsInternet, TagsOS, TagsOperator}
import com.utils.{JedisPool, TagUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

/**
  * 上下文标签
  */
object TagsContext {
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
    //获取字典集 ,进行数据的读取，处理分析数据
    val lines: RDD[String] = sc.textFile("F:\\Big-Data-22\\项目\\资料\\Spark用户画像分析\\app_dict.txt")
    val id_Name = lines.map(_.split("\t",-1))
      .filter(_.length>5).map(x => {
      (x(4), x(1))//name,id
    }).collectAsMap()
    //封装广播变量
    val NameAndid: Broadcast[collection.Map[String, String]] = sc.broadcast(id_Name)

    //获取停用词
    val stopwords = sc.textFile("F:\\Big-Data-22\\项目\\资料\\Spark用户画像分析\\stopwords.txt").map((_,1)).collectAsMap()
    //广播停用词
    val bcstopword: Broadcast[collection.Map[String, Int]] = sc.broadcast(stopwords)

    //过滤符合Id的数据
    val res = df.filter(TagUtils.OneUserId).rdd
      //接下来所有的标签都在内部实现
      .map(row => {
      //取出用户Id
      val userId = TagUtils.getOneUserId(row)
      //接下来通过row数据 打上所有标签
      //广告位类型标签
      val adList = TagsAd.makeTags(row)
//      (userId,adList)

      //App名称标签
      val AppList = TagsApp.makeTags(row,NameAndid)
//      (userId,AppList)

//      val appid = row.getAs[String]("appid")
//      //redis获取
//      val jedis: String = JedisPool.getConnections().get(appid)
//
//      TagsApp2.makeTags(row,jedis)

      //渠道标签
      val adplatformproviderList = TagsAdplatformprovider.makeTags(row)
//      (userId,adplatformproviderList)

      //设备标签
      //a)	(操作系统 -> 1)
      //b)	(联网方 -> 1)
      //c)	(运营商 -> 1)
      val OSList = TagsOS.makeTags(row)
//      (userId,OSList)
      val InternetList = TagsInternet.makeTags(row)
//      (userId,InternetList)
      val OperatorList = TagsOperator.makeTags(row)
//      (userId,OperatorList)

      //关键字标签
      val keyWordList = TagKeyWord.makeTags(row,bcstopword)
//      (userId,keyWordList)
      //地域标签
      val proCityList = TagProCity.makeTags(row)
//      (userId,proCityList)
      //上下文标签
      (userId,
        adList(0)._1,adList(0)._2,adList(1)._1,adList(1)._2,
        AppList(0)._1,AppList(0)._2,
        adplatformproviderList(0)._1,adplatformproviderList(0)._2,
        OSList(0)._1,OSList(0)._2,
        InternetList(0)._1,InternetList(0)._2,
        OperatorList(0)._1,OperatorList(0)._2,
        keyWordList,
        proCityList(0)._1,proCityList(0)._2)
    })
   res.collect().foreach(println)
  }
}
