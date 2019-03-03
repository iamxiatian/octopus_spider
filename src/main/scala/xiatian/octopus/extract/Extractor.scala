package xiatian.octopus.extract

import org.zhinang.protocol.http.UrlResponse
import xiatian.octopus.actor.{Context, ProxyIp}
import xiatian.octopus.model.{FetchItem, FetchType}
import xiatian.octopus.task.epaper.EPaperTask

/**
  * 抽取器的特质
  */
trait Extractor {
  def extract(link: FetchItem,
              context: Context,
              response: UrlResponse,
              proxyHolder: Option[ProxyIp]
             ): Either[Throwable, ExtractResult]
}

case object EmptyExtractor extends Extractor {
  def extract(link: FetchItem,
              context: Context,
              response: UrlResponse,
              proxyHolder: Option[ProxyIp]
             ): Either[Throwable, ExtractResult] =
    Right(ExtractResult(link, List.empty[FetchItem], Map.empty[String, Any]))
}

object Extractor {
  def find(link: FetchItem): Option[Extractor] = {
    link.`type` match {
      case FetchType.ArticlePage =>
        Option(ArticleExtractor)
      case FetchType.EPaper.Column =>
        //EPaperTask()
        None
      case _ => None
    }
  }
}