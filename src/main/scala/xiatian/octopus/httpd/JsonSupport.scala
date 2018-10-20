package xiatian.octopus.httpd

import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.StandardRoute
import io.circe._
import io.circe.syntax._

trait JsonSupport extends RouteSupport {
  val SUCCESS_CODE = 0
  val ERROR_CODE = 1

  def jsonOk(data: String, msg: String): String = jsonOk(data.asJson, msg)

  def jsonOk(data: Json, msg: String): String = Map(
    "code" -> SUCCESS_CODE.asJson,
    "msg" -> msg.asJson,
    "data" -> data).asJson.pretty(Printer.spaces2)

  /**
    * 输出符合layui模板格式的JSON表格数据
    *
    * @return
    */
  def jsonTable(count: Long,
                data: Seq[Json],
                code: Int = SUCCESS_CODE,
                msg: String = ""): String =
    Map[String, Json](
      "code" -> code.asJson,
      "msg" -> msg.asJson,
      "count" -> count.asJson,
      "data" -> data.asJson
    ).asJson.pretty(Printer.spaces2)

  /**
    * 输出带有status状态标记的JSON结果
    *
    * @return
    */
  def writeJsonOk(data: Json = "".asJson,
                  msg: String = "success"): StandardRoute = complete(
    HttpEntity(ContentTypes.`application/json`, jsonOk(data, msg))
  )

  def writeJsonError(msg: String): StandardRoute = complete(
    HttpEntity(ContentTypes.`application/json`, jsonError(msg))
  )

  def jsonError(msg: String): String = Map(
    "code" -> ERROR_CODE.asJson,
    "msg" -> msg.asJson).asJson.pretty(Printer.spaces2)
}