package xiatian.octopus.extract

import org.zhinang.protocol.http.UrlResponse
import xiatian.octopus.actor.ProxyIp
import xiatian.octopus.common.OctopusException
import xiatian.octopus.model.{Context, FetchItem}

object ArticleExtractor extends Extractor {
  override def extract(link: FetchItem,
                       context: Context,
                       response: UrlResponse,
                       proxyHolder: Option[ProxyIp]): Either[Throwable, ExtractResult] =
    Left(OctopusException("还未实现自动抽取"))
}
