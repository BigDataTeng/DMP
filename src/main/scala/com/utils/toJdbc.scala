package com.utils

import java.util.Properties

/**
  * Description：xxxx<br/>
  * Copyright(c),2019,Beijing<br/>
  * This program is protected by copyright laws.<br/>
  * Date:2019年08月20日 23:25
  *
  * @author Mr.Liu 
  * @version : 1.0
  */
object toJdbc {
  def getProperties() = {
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "123456")
    val url = "jdbc:mysql://cdh:3306/dmp?useUnicode=true&characterEncoding=utf8"
    (url,prop)
  }
}
