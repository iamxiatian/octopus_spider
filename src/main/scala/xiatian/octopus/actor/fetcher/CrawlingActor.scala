package xiatian.octopus.actor.fetcher

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import xiatian.octopus.actor.store.StoreMessage
import xiatian.octopus.actor.{Fetch, FetchCode, FetchResult}
import xiatian.octopus.task.FetchTask
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
    case Fetch(item, fetcherId, context, proxyHolder) =>
      if (log.isInfoEnabled) log.info(s"crawling $item")

      try {
        //只有text/html类型的网页才会继续提取内容，填充到response对象中
        println(s"fetching ${item.value}")
        val response = Http.get(item, proxyHolder, Option("text/html"))

        if (response.getCode != 200) {
          log.error(response.toString)
          sender() ! FetchResult(fetcherId, FetchCode.Error, item)
        } else if (response.getContentType != "text/html") {
          sender() ! FetchResult(fetcherId, FetchCode.Not_HTML, item)
        } else {
          //TODO: 根据设置信息：把采集到的内容保存起来
          // pageCacheActor ! CachingPage(link.url, link.`type`, response.getContent, response.getEncoding)

          FetchTask.get(item) match {
            case Some(task) =>
              val parseResult = task.parser.get.parse(item).get
              if (parseResult.data.nonEmpty) {
                storeClient ! StoreMessage(item, parseResult.data.get)
              }
              sender() ! FetchResult(fetcherId, FetchCode.Ok, item,
                parseResult.children)

            case None =>
              sender() ! FetchResult(fetcherId, FetchCode.PARSE_ERROR, item,
                message = Option(s"未识别的任务id: ${item.taskId}"))
          }
        }
      } catch {
        case e: Throwable =>
          println(s"Fetch error, url: ${item.url}, error: ${e}")
          log.error(e, s"Still got fetch error: ${item}, maybe you were blocked!")
          sender() ! FetchResult(fetcherId, FetchCode.PARSE_ERROR, item, message = Option(e.toString))
      }
  }

}