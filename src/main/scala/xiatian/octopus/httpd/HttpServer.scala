package xiatian.octopus.httpd

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.HttpOriginRange
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.stream.ActorMaterializer
import akka.util.Timeout
import ch.megard.akka.http.cors.scaladsl.CorsDirectives.cors
import ch.megard.akka.http.cors.scaladsl.settings.CorsSettings
import org.slf4j.LoggerFactory
import xiatian.octopus.actor.SpiderSystem.Master
import xiatian.octopus.common.{BuildInfo, MyConf}
import xiatian.octopus.httpd.controller.LogController

import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * HTTP Daemon: 提供外部访问的API和管理界面
  */
object HttpServer extends JsonSupport {
  val log = LoggerFactory.getLogger(HttpServer.getClass)

  implicit val timeout = Timeout(20.seconds)

  val settings = CorsSettings.defaultSettings.withAllowedOrigins(HttpOriginRange.*)

  def start(implicit system: ActorSystem = Master.system): Future[ServerBinding] = {
    implicit val materializer = ActorMaterializer()

    // needed for the future flatMap/onComplete in the end
    import system.dispatcher

    val myExceptionHandler = ExceptionHandler {
      case e: Exception =>
        extractUri { uri =>
          println(s"Request to $uri could not be handled normally")
          complete(HttpResponse(InternalServerError, entity = e.toString))
        }
    }

    val route: Route = handleExceptions(myExceptionHandler) {
      (path("") & get) {
        //complete(s"Spider ${Settings.version}")
        redirect("index.html", StatusCodes.MovedPermanently)
      } ~
        (path("build.json") & get & cors(settings)) {
          //编译信息
          complete(HttpEntity(
            ContentTypes.`application/json`,
            BuildInfo.toJson
          ))
        } ~
        LogController.routes ~
        TaskRoute.route ~
        SpiderRoute.route ~
        SystemRoute.route ~
        ProxyIpRoute.route ~
        ToolboxRoute.route ~
        pathPrefix("doc") {
          getFromDirectory("web/doc")
        } ~
        get {
          // 所有其他请求，都直接访问web目录中的对应内容
          //getFromResourceDirectory("web")
          getFromDirectory("web")
        }
    }

    println(s"Server online at http://localhost:${MyConf.apiServerPort}/")

    //启动服务，并在服务关闭时，解除端口绑定
    Http(system).bindAndHandle(route, "0.0.0.0", MyConf.apiServerPort)
  }
}
