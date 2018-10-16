package xiatian.octopus.parse

import org.jsoup.nodes.Document
import xiatian.octopus.actor.ProxyIp
import xiatian.octopus.model.FetchLink

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * 抽取器的特质
  */
trait Extractor {
  def extract(link: FetchLink,
              doc: Document,
              proxyHolder: Option[ProxyIp]
             ): Future[Either[String, ExtractResult]]
}

case object EmptyExtractor extends Extractor {
  def extract(link: FetchLink,
              doc: Document,
              proxyHolder: Option[ProxyIp]
             ): Future[Either[String, ExtractResult]] = Future(
    Right(ExtractResult(link, List.empty[FetchLink], Map.empty[String, Any]))
  )
}