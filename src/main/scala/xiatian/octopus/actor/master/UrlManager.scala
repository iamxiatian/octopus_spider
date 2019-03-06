package xiatian.octopus.actor.master

import java.text.SimpleDateFormat

import io.circe.Json
import io.circe.syntax._
import org.slf4j.LoggerFactory
import xiatian.octopus.common.MyConf
import xiatian.octopus.model.{FetchItem, FetchType}
import xiatian.octopus.storage.master.{StatsDb, _}
import xiatian.octopus.task.FetchTask
import xiatian.octopus.util.HashUtil

import scala.concurrent.Future

/**
  * Url Manager
  *
  * @author Tian Xia
  *         Dec 05, 2016 13:45
  */
object UrlManager extends MasterConfig {
  val log = LoggerFactory.getLogger(UrlManager.getClass)
  val dayFormat = new SimpleDateFormat("yyMMdd")
  val hourFormat = new SimpleDateFormat("yyMMddHH")

  def markFetched(link: FetchItem) = {
    FetchedSignatureDb.put(link.urlHash)
    removeFetching(link)
  }

  def removeFetching(link: FetchItem) =
    FetchingSignatureDb.remove(link.urlHash)

  /**
    * 设置url, 该主键10分钟后过期. urlFetching表示的是正在由某个爬虫客户端在抓取的链接，避免
    * 有爬虫在抓取的时候，该URL被二次处理。具体调用见FetchMasterActor
    *
    * @return
    */
  def markFetching(link: FetchItem) =
    FetchingSignatureDb.put(link.urlHash)

  /**
    * 标记死链接url,避免重复抓取
    */
  def markDead(link: FetchItem): Unit = {
    removeFetching(link)
    Future.successful {
      BadLinkDb.saveUrl(link.url)
      markUnknownHost(link.getHost)
    }
  }

  private def markUnknownHost(host: String) = BadLinkDb.saveHost(host)

  /**
    * 链接存入全局队列中，等待抓取, 如果插入，返回true，否则返回false.
    * 如果设置了tryFillBucket，会尝试把数据注入到FetcherController的桶中
    * 首页是0，如果仅抓取首页及其文章，应该把深度设为0.
    */
  def pushLink(link: FetchItem,
               tryFillBucket: Boolean): Boolean =
    if (BucketController.inBucket(link)
      || isFetching(link)
      || isDead(link)
      || isFetched(link)
      || CrawlDb.has(link)
    ) {
      println("skip push link")
      false
    } else if (tryFillBucket && BucketController.fillLink(link)) {
      true //先注入到爬行队列中，如果队列已满，注入失败，则保存到爬行数据库中
    } else {
      CrawlDb.pushLink(link)
    }

  /**
    * url是否已经被抓取过
    */
  def isFetched(link: FetchItem) = {
    val expiredSeconds: Long = FetchTask.get(link).map {
      task =>
        task.nextFetchSeconds(link).getOrElse(MyConf.MaxTimeSeconds)
    }.getOrElse(MyConf.MaxTimeSeconds)

    FetchedSignatureDb.has(link.urlHash, expiredSeconds)
  }

  /**
    * 是否在数据页面的待抓取队列中
    */
  def isFetching(link: FetchItem) =
    FetchingSignatureDb.has(link.urlHash, 120L)

  /**
    * Expired time: 5 minutes (300 seconds)
    *
    */
  def isDead(link: FetchItem) = BadLinkDb.hasUrl(link.url, 300)

  /**
    * 把原来从队列中出来的链接重新归还回队列, 插入成功，返回Future(1),否则返回Future(0)
    *
    * @param link
    * @return
    */
  def pushLinkBack(link: FetchItem): Boolean =
    if (link == null) {
      true
    } else if (!CrawlDb.has(link)) {
      CrawlDb.returnLink(link)
    } else {
      false
    }

  /**
    * 把链接插入到队列中，返回插入后队列的长度, 0表示未插入
    *
    * @param link
    * @return
    */
  def pushToCrawlDb(link: FetchItem): Boolean =
    if (CrawlDb.has(link)) false else CrawlDb.pushLink(link)

  def popLink(t: FetchType): Option[FetchItem] = CrawlDb.popLink(t)

  def queueSize(t: FetchType) = CrawlDb.queueSize(t)

  def countSuccess(link: FetchItem) = Future.successful {
    countStats(s"p:s:${link.`type`.id}")
  }

  def countFailure(link: FetchItem) = Future.successful {
    countStats(s"p:f:${link.`type`.id}")
  }

  /**
    * 记录统计信息
    *
    * @return
    */
  def countStats(key: String, incValue: Int = 1) = Future.successful {
    val (day, hour) = currentDayAndHour()
    StatsDb.inc(s"${key}:${day}", incValue)
    StatsDb.inc(s"${key}:${hour}", incValue)
  }

  private def currentDayAndHour(): (String, String) = {
    val d = new java.util.Date
    (dayFormat.format(d), hourFormat.format(d))
  }

  /**
    * 清空数据
    */
  def clear(removeFingerprint: Boolean = false) = {

  }


  ///////////////////////////////////////
  // Report information
  ///////////////////////////////////////

  /**
    * 返回最近2天和3小时之内的统计数据, 保存到JSON对象中
    *
    * @return
    */
  def report(): Future[Map[String, Json]] = Future.successful {
    val dayResult: List[(String, Json)] = lastDays(2).map {
      case (label, field) =>
        val value: Json = Map[String, Json](
          "success" ->
            FetchType.all.map { t =>
              val count = StatsDb.get(s"p:s:${field}")
              (t.name, count.asJson)
            }.toMap[String, Json].asJson,

          "failure" ->
            FetchType.all.map { t =>
              val count = StatsDb.get(s"p:f:${field}")
              (t.name, count.asJson)
            }.toMap[String, Json].asJson
        ).asJson

        (label, value)
    }

    val hourResult: List[(String, Json)] = lastHours(3).map {
      case (label, field) =>
        val value: Json = Map[String, Json](
          "success" ->
            FetchType.all.map { t =>
              val count = StatsDb.get(s"p:s:${field}")
              (t.name, count.asJson)
            }.toMap[String, Json].asJson,

          "failure" ->
            FetchType.all.map { t =>
              val count = StatsDb.get(s"p:f:${field}")
              (t.name, count.asJson)
            }.toMap[String, Json].asJson
        ).asJson

        (label, value)
    }

    Map(
      "days" -> dayResult.asJson,
      "hours" -> hourResult.asJson
    )
  }

  /**
    * 返回最近days天的标签和对应的查询主键的名称，如：
    * (2016/12/12, 161212)
    */
  def lastDays(days: Int): List[(String, String)] = {
    val df = new SimpleDateFormat("yyyy/MM/dd")
    val d = System.currentTimeMillis()
    (0 until days) map (i => {
      val currentDate = new java.util.Date(d - (86400000L * i))
      (df.format(currentDate), dayFormat.format(currentDate))
    }) toList
  }

  /**
    * 返回最近hours小时内的标签和对应的查询主键名称，如：
    * (2016/12/12 11:00-11:59, 16121211)
    */
  def lastHours(hours: Int): List[(String, String)] = {
    val df = new SimpleDateFormat("yyyy/MM/dd HH:00 - HH:59")
    val d = System.currentTimeMillis()
    (0 until hours) map (i => {
      val currentDate = new java.util.Date(d - (3600000L * i))
      (df.format(currentDate), hourFormat.format(currentDate))
    }) toList
  }

  def main(args: Array[String]): Unit = {
    val url = "http://paper.people.com.cn/rmrb/html/2019-03/05/nw.D110000renmrb_20190305_8-10.htm"
    FetchedSignatureDb.open()
    val existed = FetchedSignatureDb.has(HashUtil.hashAsBytes(url), MyConf.MaxTimeSeconds)
    println(existed)
    FetchedSignatureDb.close()
  }
}
