package xiatian.octopus.httpd

import akka.http.scaladsl.server.Directives.{path, _}
import akka.http.scaladsl.server.Route
import ch.megard.akka.http.cors.scaladsl.CorsDirectives.cors
import org.slf4j.LoggerFactory
import xiatian.octopus.httpd.HttpServer.settings
import xiatian.octopus.storage.master.XmlTaskDb

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * 任务相关的路由处理
  *
  * @author Tian Xia
  *         School of IRM, Renmin University of China.
  *         Jan 25, 2018 22:44
  */
object TaskRoute extends JsonSupport {
  val log = LoggerFactory.getLogger("TaskRoute")

  def route: Route =
    (path("api" / "task" / "remove_all") & delete & cors(settings)) {
      log.error("Caution: you are removing all tasks!!!")
      onSuccess(Future.successful(XmlTaskDb.clear)) {
        case Success(_) =>
          writeJsonOk(msg = "all boards have been removed.")
        case Failure(e) =>
          writeJsonError(e.toString)
      }
    } ~
      (path("api" / "task" / "remove")
        & delete
        & parameter('id.as[String])
        & cors(settings)) {
        id: String =>
          log.warn(s"Caution: try to remove task $id")
          //用HTTPie测试： http
          XmlTaskDb.delete(id)
          writeJsonOk(msg = s"Board ${id} has been removed.")
      }
}

