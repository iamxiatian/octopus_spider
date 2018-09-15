package xiatian.spider.actor.master

import xiatian.spider.actor.master.db._

/**
  * FetchMaster用到的数据库
  */
object MasterDb {
  val dbs: List[Db] = List(
    CrawlDb,
    SignatureDb,
    XmlTaskDb,
    StatsDb,
    BadLinkDb,
    FetchLogDb,
    WaitDb
  )


  def open(): Unit = dbs.foreach(_.open())

  def close(): Unit = dbs.foreach(_.close())

}
