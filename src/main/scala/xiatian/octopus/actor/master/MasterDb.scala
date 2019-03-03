package xiatian.octopus.actor.master

import xiatian.octopus.storage.Db
import xiatian.octopus.storage.master._

/**
  * FetchMaster用到的数据库
  */
object MasterDb {
  val dbs: List[Db] = List(
    CrawlDb,
    FetchedSignatureDb,
    FetchingSignatureDb,
    TaskDb,
    StatsDb,
    BadLinkDb,
    FetchLogDb,
    WaitDb
  )


  def open(): Unit = dbs.foreach(_.open())

  def close(): Unit = dbs.foreach(_.close())

}
