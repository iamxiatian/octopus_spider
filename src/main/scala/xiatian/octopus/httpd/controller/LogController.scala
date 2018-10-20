package xiatian.octopus.httpd.controller

import akka.http.scaladsl.model.ContentTypes.`application/json`
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.server.Directives.{complete, path, _}
import akka.http.scaladsl.server.Route
import ch.megard.akka.http.cors.scaladsl.CorsDirectives.cors
import io.circe._
import io.circe.syntax._
import org.slf4j.LoggerFactory
import xiatian.octopus.httpd.{HttpServer, JsonSupport}
import xiatian.octopus.storage.master.{BadLinkDb, FetchLogDb, WaitDb}

/**
  * 负责URL队列处理的桶的相关路由
  *
  * @author Tian Xia
  *         School of IRM, Renmin University of China.
  *         Jan 25, 2018 22:44
  */
object LogController extends JsonSupport {
  //允许所有来源的请求
  val settings = HttpServer.settings

  val LOG = LoggerFactory.getLogger("LogController")

  def routes: Route = historyRoute ~ deadLinkRoute

  private def historyRoute: Route =
    (path("api" / "log" / "link" / "history_list.json") & get & cors(settings)) {
      val items: Seq[Json] = FetchLogDb.listLogs(1, 100).map {
        log =>
          Map(
            "fetcherId" -> log.fetcherId.asJson,
            "linkType" -> log.linkType.asJson,
            "url" -> log.url.asJson,
            "code" -> log.code.asJson,
            "fetchTime" -> log.fetchTime.toString("yyyy-MM-dd HH:mm:ss").asJson
          ).asJson
      }

      complete(HttpEntity(`application/json`, jsonTable(items.size, items)))
    }

  private def deadLinkRoute: Route =
    (path("api" / "log" / "link" / "dead_list.json") & get & cors(settings)) {
      val deadUrls = BadLinkDb.listDeadUrls(1000)

      complete(HttpEntity(`application/json`,
        jsonTable(deadUrls.size,
          deadUrls.map(t =>
            Map(
              "url" -> t._1.asJson,
              "fetchTime" -> t._2.asJson
            ).asJson
          ))
      ))
    }

  private def clearRoute: Route =
    (path("api" / "log" / "clear.do") & post & cors(settings)) {
      FetchLogDb.clear
      WaitDb.clear
      writeJson("操作完毕。")
    }
}

