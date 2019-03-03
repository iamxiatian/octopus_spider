package xiatian.octopus.extract

import org.zhinang.protocol.http.UrlResponse
import xiatian.octopus.actor.ProxyIp
import xiatian.octopus.model.{Context, FetchItem}
import xiatian.octopus.util.HtmlUtil

import scala.util.Try

/**
  * 中心网页链接的抽取，中心网页的代表类型是栏目页面、首页等. HubExtractor将抽取出
  * 页面内包含的所有链接，传到Master，后续Master会进一步根据任务类型和属性信息，
  * 去除需要排除的链接后，将有效链接进行保存。
  *
  * 注意：Extractor运行在Fetcher客户端，不能直接访问Master端的Task细节信息
  */
object HubExtractor extends Extractor {
  override def extract(link: FetchItem,
                       context: Context,
                       response: UrlResponse,
                       proxyHolder: Option[ProxyIp]
                      ): Either[Throwable, ExtractResult] = Try {
    val urlAnchorPairs: Map[String, String] = HtmlUtil.parseHrefs(
      link.url,
      response getEncoding,
      response getContent)


    //val childLinks = context.task.makeChildLinks(link, urlAnchorPairs)
    ExtractResult(link, List.empty, proxyHolder = proxyHolder)
  }.toEither
}
