package xiatian.octopus.httpd

import akka.http.scaladsl.server.Directives.{path, _}
import akka.http.scaladsl.server.Route
import ch.megard.akka.http.cors.scaladsl.CorsDirectives.cors
import io.circe._
import io.circe.parser._
import io.circe.syntax._
import org.joda.time.DateTime
import xiatian.octopus.actor.ProxyIp
import xiatian.octopus.actor.master.ProxyIpPool
import xiatian.octopus.httpd.HttpServer.{log, settings}

/**
  * 代理IP相关的HTTP路由，使用说明请参考API.md
  *
  * @author Tian Xia
  *         School of IRM, Renmin University of China.
  *         Jul 26, 2017 22:44
  */
object ProxyIpRoute extends JsonSupport {

  def route: Route =
    (path("api" / "proxy_ip" / "batch_add") & post & cors(settings)) {
      entity(as[String]) {
        text =>
          log.info(s"try to add new proxy ip addresses: $text")
          parse(text).getOrElse(Json.Null).asArray match {
            case Some(items) =>
              val proxies: Seq[ProxyIp] = items.map {
                item: Json =>
                  val p = item.asObject.get
                  val host: String = p("host").get.as[String].getOrElse("")
                  val port: Int = p("port").get.as[Int].getOrElse(0)
                  val duration: Long = p("duration").get.as[Long].getOrElse(0)
                  ProxyIp(host, port, duration * 1000L + System.currentTimeMillis())
              }.filter(_.host.nonEmpty)

              proxies foreach ProxyIpPool.addProxy

              writeJsonOk(msg = s"add ${proxies.size} proxy addresses.")
            case None =>
              writeJsonOk(msg = "No proxy inside.")
          }
      }
    } ~
      (path("api" / "proxy_ip" / "clear") & delete) {
        ProxyIpPool.clear
        writeJsonOk(msg = "clear command has submitted.")
      } ~
      (path("api" / "proxy_ip" / "list") & get) {
        val data: Json =
          ProxyIpPool.listProxyIps().map {
            ip =>
              Map[String, Json](
                "host" -> ip.host.asJson,
                "port" -> ip.port.asJson,
                "expiredDate" -> (new DateTime(ip.expiredTimeInMillis)
                  .toString("yyyy-MM-dd HH:mm:ss")).asJson
              ).asJson
          }.asJson

        writeJsonOk(data)
      }
}
