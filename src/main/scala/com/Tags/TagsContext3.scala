package com.Tags

import com.typesafe.config.ConfigFactory
import com.utils.TagUtils
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/**
  * 上下文标签
  */
object TagsContext3 {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("目录不匹配，退出程序")
      sys.exit()
    }
    val Array(inputPath) = args
    //创建上下文
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    //todo 调用Hbase API
    //加载配置文件
    val load = ConfigFactory.load()
    //获取hbase.TableName
    val hbaseTableName = load.getString("hbase.TableName")
    // 创建Hadoop任务
    val configuration = sc.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum",load.getString("hbase.host"))
    // 创建HbaseConnection
    val hbconn = ConnectionFactory.createConnection(configuration)
    val hbadmin = hbconn.getAdmin
    //判断表格是否存在
    if(!hbadmin.tableExists(TableName.valueOf(hbaseTableName))){
      // 创建表
      val tabledescriptor =new HTableDescriptor(TableName.valueOf(hbaseTableName))
      val descriptor = new HColumnDescriptor("tags")
      tabledescriptor.addFamily(descriptor)
      hbadmin.createTable(tabledescriptor)
      hbadmin.close()
      hbconn.close()
    }
    // 创建JobConf
    val jobconf = new JobConf(configuration)
    // 指定输出类型和表
    jobconf.setOutputFormat(classOf[TableOutputFormat])
    jobconf.set(TableOutputFormat.OUTPUT_TABLE,hbaseTableName)

    //读取数据
    val df = sQLContext.read.parquet(inputPath)
    //获取字典集 ,进行数据的读取，处理分析数据
    val lines: RDD[String] = sc.textFile("F:\\Big-Data-22\\项目\\资料\\Spark用户画像分析\\app_dict.txt")
    val id_Name = lines.map(_.split("\t", -1))
      .filter(_.length > 5).map(x => {
      (x(4), x(1)) //name,id
    }).collectAsMap()
    //封装广播变量
    val broadcast: Broadcast[collection.Map[String, String]] = sc.broadcast(id_Name)

    //获取停用词
    val stopwords = sc.textFile("F:\\Big-Data-22\\项目\\资料\\Spark用户画像分析\\stopwords.txt").map((_, 1)).collectAsMap()
    //广播停用词
    val bcstopword: Broadcast[collection.Map[String, Int]] = sc.broadcast(stopwords)

    // 过滤符合Id的数据
    df.filter(TagUtils.OneUserId).rdd
      // 接下来所有的标签都在内部实现
      .map(row=>{
      // 取出用户Id
      val userId = TagUtils.getAllUserId(row)
      // 接下来通过row数据 打上 所有标签（按照需求）
      val adList = TagsAd.makeTags(row)
      val appList = TagsApp2.makeTags(row,broadcast)
      val keywordList = TagKeyWord.makeTags(row,bcstopword)
      val dvList = TagDevice.makeTags(row)
      val loactionList = TagProCity.makeTags(row)
      val business = TagBusiness.makeTags(row)
      (userId,adList++appList++keywordList++dvList++loactionList++business)
    })
      .reduceByKey((list1,list2)=>
        // List(("lN插屏",1),("LN全屏",1),("ZC沈阳",1),("ZP河北",1)....)
        (list1:::list2)
          // List(("APP爱奇艺",List()))
          .groupBy(_._1)
          .mapValues(_.foldLeft[Int](0)(_+_._2))
          .toList
      )
  }
}
