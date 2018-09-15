package xiatian.api

import akka.http.scaladsl.model.ContentTypes.`application/json`
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.server.Directives.{complete, path, _}
import akka.http.scaladsl.server.Route
import ch.megard.akka.http.cors.scaladsl.CorsDirectives.cors
import io.circe._
import io.circe.syntax._
import org.jsoup.Jsoup
import xiatian.api.HttpApiServer.{log, settings}
import xiatian.common.util.HtmlUtil

import scala.collection.JavaConverters._


object ToolboxRoute extends JsonSupport {

  def route: Route =
    (path("api" / "page" / "container_links") & get & cors(settings)
      & parameter('u.as[String], 'q.as[String])) {
      //输出一个网页中包含的所有链接, 调用方式：
      //http://ip/api/page/find_links?u=http://news.baidu.com&boardId=1
      (url: String, cssQuery: String) =>
        log.info(s"find all links from $url  (container: $cssQuery)")

        val doc = Jsoup.connect(url).get()
        val elements = doc.select(cssQuery)

        val links: Seq[Json] = elements.asScala
          .toList
          .flatMap(HtmlUtil.extractHrefs)
          .map {
            link =>
              Map(
                "url" -> (link._1).asJson,
                "anchor" -> (link._2).asJson
              ).asJson
          }

        writeJsonOk(links.asJson)
    }
}
