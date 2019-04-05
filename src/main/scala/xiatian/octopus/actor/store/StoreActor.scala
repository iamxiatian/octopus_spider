package xiatian.octopus.actor.store

import akka.actor.Actor
import com.google.common.hash.Hashing
import xiatian.octopus.actor.ActorWatching
import xiatian.octopus.common.{Logging, Symbols}
import xiatian.octopus.model.FetchItem
import xiatian.octopus.parse.ParsedData
import xiatian.octopus.task.epaper.{EPaperArticle, EPaperArticleDb}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

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
    case StoreMessage(item, parsedData) =>
      Store.save(item, parsedData)
  }
}

case class StoreMessage(item: FetchItem, parsedData: ParsedData)

/**
  * 执行具体保存的逻辑代码
  */
object Store extends Logging {
  def save(item: FetchItem, parsedData: ParsedData) = {
    parsedData match {
      case article: EPaperArticle =>
        //println(s"${article.title}")
        Await.result(EPaperArticleDb.save(article), Duration.Inf)
        print(Symbols.SAVE_DB)
      //process
      case _ =>
        LOG.error(s"竟不知如何保存该类型的数据~~~ $item")
    }
  }
}