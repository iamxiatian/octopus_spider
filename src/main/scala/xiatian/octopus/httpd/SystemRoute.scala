package xiatian.octopus.httpd

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.{complete, path, _}
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import ch.megard.akka.http.cors.scaladsl.CorsDirectives.cors
import org.slf4j.LoggerFactory
import xiatian.octopus.actor.SpiderSystem.Master
import xiatian.octopus.actor.master.{BucketController, UrlManager}
import xiatian.octopus.actor.{FetchStatsReply, FetchStatsRequest}
import xiatian.octopus.httpd.HttpServer.settings

/**
  * 系统整体相关的HTTP路由信息，包括整体统计信息，服务停止处理等。
  *
  * @author Tian Xia
  *         School of IRM, Renmin University of China.
  *         Jul 26, 2017 22:44
  */
object SystemRoute extends JsonSupport {
  val log = LoggerFactory.getLogger("SystemRoute")

  def route: Route =
    (path("api" / "stats.json") & get & cors(settings)) {
      import HttpServer.timeout

      if (Master.fetchMasterHolder.nonEmpty) {
        onSuccess(Master.fetchMasterHolder.get ? FetchStatsRequest()) {
          case FetchStatsReply(msg: String) =>
            complete(StatusCodes.OK, msg)
          case _ =>
            complete(StatusCodes.InternalServerError)
        }
      } else {
        writeJsonError("FetchMaster没有启动，无法获取运行信息")
      }
    } ~
      (path("api" / "shutdown") & get) {
        val timer = new java.util.Timer
        timer.schedule(
          new java.util.TimerTask() {
            def run = Master.shutdown()
          }, 3000L)

        complete(StatusCodes.OK, "已发送停止服务命令, 3秒后停止运行...")
      } ~
      (path("api" / "clear") & get & parameter('rm_finger.as[Boolean] ? false)) {
        removeFingerprint =>
          log.warn("Caution: try to clear all data.")
          UrlManager.clear(removeFingerprint)

          //同时清空桶中的数据
          BucketController.empty()

          writeJsonOk(msg = "Finished")
      }
}

