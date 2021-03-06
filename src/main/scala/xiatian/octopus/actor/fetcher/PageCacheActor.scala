package xiatian.octopus.actor.fetcher

import akka.actor.{Actor, ActorLogging}
import xiatian.octopus.model.FetchType

case class CachingPage(url: String, urlType: FetchType, content: Array[Byte], encoding: String)

/**
  * 页面缓存Actor
  */
class PageCacheActor extends Actor with ActorLogging {
  override def receive: Receive = {
    case CachingPage(url, urlType, content, encoding) =>
      //@TODO
      println("TODO...")
    //      PageCacheDb.save(url, urlType, content, encoding).onComplete {
    //        case Success(_) =>
    //          log.info(s"cached page $url")
    //        case Failure(e) =>
    //          log.error(s"cache error for url: $url >>> $e")
    //      }
  }
}
