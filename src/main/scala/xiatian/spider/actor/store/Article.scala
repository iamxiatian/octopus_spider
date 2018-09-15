package xiatian.spider.actor.store

import java.util.Date

/**
  * 文章对象，主要用于从数据库中读取数据，保存在该对象中，用于展示查看
  *
  * @author Tian Xia
  *         May 10, 2017 18:03
  */
@Deprecated
case class Article(id: String,
                   url: String,
                   title: String,
                   site: String,
                   fetchTime: Date,
                   content: String)


