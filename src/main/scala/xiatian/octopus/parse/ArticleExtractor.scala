package xiatian.octopus.parse

import org.zhinang.protocol.http.UrlResponse
import xiatian.octopus.actor.{Context, ProxyIp}
import xiatian.octopus.common.OctopusException
import xiatian.octopus.model.FetchLink

object ArticleExtractor extends Extractor {
  override def extract(link: FetchLink,
                       context: Context,
                       response: UrlResponse,
                       proxyHolder: Option[ProxyIp]): Either[Throwable, ExtractResult] =
    Left(OctopusException("还未实现自动抽取"))
}
