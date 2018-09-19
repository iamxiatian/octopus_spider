package xiatian.spider.actor.store

import akka.actor.{Actor, ActorLogging}
import com.google.common.hash.Hashing
import xiatian.spider.actor.ActorWatching
import xiatian.spider.parse.ExtractResult

import scala.util.{Failure, Success, Try}

/**
  * 保存抓取到的文章数据的Actor，该Actor会直接链接配置文件中指定的数据库保存数据。
  *
  * 没有放到CrawlActor中直接保存，是考虑到未来可能会通过异步消息传送方式，把要保存的内容传递
  * 到单独的一台服务器上进行保存。
  *
  * FIXME: 目前支持两种格式的数据库：MongoDB和MySQL，通过Settings.dbType确定具体类型
  *
  * @author Tian Xia
  *         Dec 03, 2016 23:28
  */
class StoreActor extends Actor with ActorWatching {
  val hashAsInt = (url: String) => Hashing.sha256()
    .hashBytes(url.getBytes("utf-8"))
    .asInt().abs

  override def receive: Receive = {
    case result: ExtractResult =>
      Try(result.save()) match {
        case Success(_) =>
        case Failure(e) =>
          e.printStackTrace()
          log.error(e, "save error!")
      }
  }
}
