package com.utils

/**
  * 打标签的统一接口
  */
trait Tag {
  //List[(String,Int)] 标签key，value
  def makeTags(args:Any*):List[(String,Int)]
}
