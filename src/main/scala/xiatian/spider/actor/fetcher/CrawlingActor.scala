package xiatian.spider.actor.fetcher

import java.io.ByteArrayInputStream

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import xiatian.spider.actor.{Fetch, FetchCode, FetchResult}
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
          sender() ! FetchResult(fetcherId, FetchCode.Error, link)
        } else if (response.getContentType != "text/html") {
          sender() ! FetchResult(fetcherId, FetchCode.Not_HTML, link)
        } else {
          //TODO: 根据设置信息：把采集到的内容保存起来
          // pageCacheActor ! CachingPage(link.url, link.`type`, response.getContent, response.getEncoding)

          val doc: Document = Jsoup.parse(
            new ByteArrayInputStream(response.getContent),
            response.getEncoding,
            link.url
          )

          val extractor = link.`type`.extractor

          Await.result(extractor.extract(link, doc, proxyHolder), 60 seconds) match {
            case Left(msg) =>
              sender() ! FetchResult(fetcherId, FetchCode.Error, link, message = Some(msg))

            case Right(result) =>
              storeClient ! result
              sender() ! FetchResult(fetcherId, FetchCode.Ok, link, result.childLinks)
          }
        }
      } catch {
        case e: Throwable =>
          println(s"Fetch error, url: ${link.url}, error: ${e}")
          log.error(e, s"Still got fetch error: ${link}, maybe you were blocked!")
          sender() ! FetchResult(fetcherId, FetchCode.PARSE_ERROR, link, message = Some(e.toString))
      }
  }

}