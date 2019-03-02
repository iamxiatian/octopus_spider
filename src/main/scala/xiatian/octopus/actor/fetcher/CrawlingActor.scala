package xiatian.octopus.actor.fetcher

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import xiatian.octopus.actor.{Fetch, FetchCode, FetchResult}
import xiatian.octopus.parse.Extractor
import xiatian.octopus.util.Http

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
        val response = Http.get(link, proxyHolder, Option("text/html"))

        if (response.getCode != 200) {
          log.error(response.toString)
          sender() ! FetchResult(fetcherId, FetchCode.Error, link)
        } else if (response.getContentType != "text/html") {
          sender() ! FetchResult(fetcherId, FetchCode.Not_HTML, link)
        } else {
          //TODO: 根据设置信息：把采集到的内容保存起来
          // pageCacheActor ! CachingPage(link.url, link.`type`, response.getContent, response.getEncoding)

          Extractor.find(link) match {
            case Some(extractor) =>
              extractor.extract(link, context, response, proxyHolder) match {
                case Left(e) =>
                  sender() ! FetchResult(fetcherId, FetchCode.PARSE_ERROR, link,
                    message = Option(e.toString))

                case Right(result) =>
                  storeClient ! result
                  sender() ! FetchResult(fetcherId, FetchCode.Ok, link, result.childLinks)
              }
            case None =>
              sender() ! FetchResult(fetcherId, FetchCode.PARSE_ERROR, link,
                message = Option("未找到抽取器"))
          }
        }
      } catch {
        case e: Throwable =>
          println(s"Fetch error, url: ${link.url}, error: ${e}")
          log.error(e, s"Still got fetch error: ${link}, maybe you were blocked!")
          sender() ! FetchResult(fetcherId, FetchCode.PARSE_ERROR, link, message = Option(e.toString))
      }
  }

}