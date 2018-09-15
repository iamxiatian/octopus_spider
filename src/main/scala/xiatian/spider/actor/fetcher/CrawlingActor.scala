package xiatian.spider.actor.fetcher

import java.io.ByteArrayInputStream

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import xiatian.spider.actor.{Fetch, FetchCode, FetchFinished}
import xiatian.spider.model._
import xiatian.spider.tool.Http

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * 真正执行抓取的爬虫Actor
  *
  * @param storeClient
  */
class CrawlingActor(storeClient: ActorRef) extends Actor with ActorLogging {
  //页面缓存Actor
  val pageCacheActor: ActorRef = context actorOf Props(new PageCacheActor())

  override def receive: Receive = {
    case Fetch(link, fetcherId, context, proxyHolder) =>
      if (log.isInfoEnabled) log.info(s"crawling $link")

      try {

        //只有text/html类型的网页才会继续提取内容，填充到response对象中
        val response = Http.get(link, proxyHolder, "text/html")

        if (response.getCode != 200) {
          log.error(response.toString)
          sender() ! FetchFinished(link, List.empty, FetchCode.Error, fetcherId)
        } else if (response.getContentType != "text/html") {
          sender() ! FetchFinished(link, List.empty, FetchCode.Not_HTML, fetcherId)
        } else {
          //把采集到的内容保存起来
          pageCacheActor ! CachingPage(link.url, link.`type`, response.getContent, response.getEncoding)

          val doc: Document = Jsoup.parse(
            new ByteArrayInputStream(response.getContent),
            response.getEncoding,
            link.url
          )

          val extractor = link.`type`.extractor

          Await.result(extractor.extract(link, doc, proxyHolder), 60 seconds) match {
            case Left(msg) =>
              sender() ! FetchFinished(link, List.empty[FetchLink],
                FetchCode.Error, fetcherId, Some(msg))

            case Right(result) =>
              storeClient ! result
              sender() ! FetchFinished(link, result.childLinks, FetchCode.Ok, fetcherId)
          }
        }
      } catch {
        case e: Throwable =>
          println(s"Fetch error, url: ${link.url}, error: ${e}")
          log.error(e, s"Still got fetch error: ${link}, maybe you were blocked!")
          sender() ! FetchFinished(link, List.empty[FetchLink],
            FetchCode.PARSE_ERROR, fetcherId, Some(e.toString))
      }
  }

}