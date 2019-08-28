package com.terminaleQuipment

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}

/**
  * 3.2.2 需求指标四之一 操作系
  * 操作系统:Andriod、IOS、其他
  * 18	client: Int,	设备类型 （1：android 2：ios 3：wp）
  */
object OsCt {
  def main(args: Array[String]): Unit = {
    // 判断路径是否正确
    if(args.length != 1){
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    // 创建一个集合保存输入和输出目录
    val Array(inputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    // 创建执行入口
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    // 设置压缩方式 使用Snappy方式进行压缩
    sQLContext.setConf("spark.sql.parquet.compression.codec","snappy")
    // 读取本地文件
    val df = sQLContext.read.parquet(inputPath)
    // 注册临时表
    df.createOrReplaceTempView("log")
    // 指标统计
    val res = sQLContext.sql("select " +
      "case when client=1 then 'Android' when client=2 then'IOS' else '其他' end as `操作系统`," +
      "sum(case when requestmode=1 and processnode>=1 then 1 else 0 end)as `总请求`," +
      "sum(case when requestmode=1 and processnode>=2 then 1 else 0 end)as `广告请求`," +
      "sum(case when requestmode=1 and processnode=3 then 1 else 0 end)as `参与竞价数`," +
      "sum(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end)as `竞价成功率`," +
      "sum(case when iseffective=1 and isbilling=1 and iswin=1 and adorderid<>0 then 1 else 0 end)as `展示量`," +
      "sum(case when requestmode=2 and iseffective=1 then 1 else 0 end)as `点击量`," +
      "sum(case when requestmode=3 and iseffective=1 then 1 else 0 end)as `点击率`," +
      "sum(case when iseffective=1 and isbilling=1 and iswin=1 then WinPrice/1000 else 0 end)as `广告成本`," +
      "sum(case when iseffective=1 and isbilling=1 and iswin=1 then adpayment/1000 else 0 end)as `广告消费` " +
      "from log group by  client")
    res.show()
    //将结果写入mysql
    val load = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user",load.getString("jdbc.user"))
    prop.setProperty("password",load.getString("jdbc.password"))
    res.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"),"OsCt",prop)
  }
}
