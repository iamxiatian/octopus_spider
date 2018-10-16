package xiatian.octopus.httpd

import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.StandardRoute

/**
  * 增强路由支持, 简化语法
  */
trait RouteSupport {

  def writeJson(jsonText: String) = complete(
    HttpEntity(ContentTypes.`application/json`, jsonText)
  )

  def writeHtml(html: String): StandardRoute = complete(
    HttpEntity(ContentTypes.`text/html(UTF-8)`, html)
  )

  def writeText(text: String): StandardRoute = complete(
    HttpEntity(ContentTypes.`text/plain(UTF-8)`, text)
  )
}
