package xiatian.octopus.parse

import org.zhinang.protocol.http.UrlResponse
import xiatian.octopus.actor.{Context, ProxyIp}
import xiatian.octopus.model.{ArticleFetchType, FetchLink}

/**
  * 抽取器的特质
  */
trait Extractor {
  def extract(link: FetchLink,
              context: Context,
              response: UrlResponse,
              proxyHolder: Option[ProxyIp]
             ): Either[Throwable, ExtractResult]
}

case object EmptyExtractor extends Extractor {
  def extract(link: FetchLink,
              context: Context,
              response: UrlResponse,
              proxyHolder: Option[ProxyIp]
             ): Either[Throwable, ExtractResult] =
    Right(ExtractResult(link, List.empty[FetchLink], Map.empty[String, Any]))
}

object Extractor {
  def find(link: FetchLink): Option[Extractor] = {
    link.`type` match {
      case ArticleFetchType =>
        Option(ArticleExtractor)
      case _ => None
    }
  }
}