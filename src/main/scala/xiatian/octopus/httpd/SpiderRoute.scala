package xiatian.octopus.httpd

import akka.http.scaladsl.server.Directives.{path, _}
import akka.http.scaladsl.server.Route
import ch.megard.akka.http.cors.scaladsl.CorsDirectives.cors
import io.circe._
import io.circe.syntax._
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import xiatian.octopus.actor.master.BucketController
import xiatian.octopus.httpd.HttpServer.settings
import xiatian.octopus.model.LinkType
import xiatian.octopus.storage.master.WaitDb

import scala.collection.JavaConverters._

/**
  * 负责URL队列处理的桶的相关路由
  *
  * @author Tian Xia
  *         School of IRM, Renmin University of China.
  *         Jan 25, 2018 22:44
  */
object SpiderRoute extends JsonSupport {
  val log = LoggerFactory.getLogger("BucketRoute")

  def route: Route = bucketRoute ~ waitUrlRoute ~ promoteWaitUrls

  /**
    * 查看桶内数据的路由
    */
  private def bucketRoute: Route =
    (path("api" / "bucket" / "list.json") & get & cors(settings)) {
      val items: Seq[Map[String, Json]] = BucketController.buckets.map {
        bucket =>
          bucket.queues.asScala.map {
            case (typeId, q) =>
              val linkType = LinkType(typeId)
              val links: Seq[Json] = bucket.getLinks(linkType).map {
                link =>
                  Map(
                    "url" -> link.url.asJson,
                    "params" -> link.params
                      .map {
                        case (k, v) => (k, v.asJson)
                      }.asJson
                  ).asJson
              }
              //每个类型的链接，和对应的所有链接数组，作为一条记录，用于构成Map
              (linkType.name, links)
          }
            .filter(_._2.size > 0) //过滤掉元素为空的类型
            .toMap[String, Seq[Json]]
            .map {
              //把type -> links的对，变为type->json，方便circe处理
              case (name, links) => (name, links.asJson)
            } + ("id" -> bucket.idx.asJson) //再合并上桶的编号，方便了解type->links的对，隶属于哪个桶
      }.filter(_.size > 1) //过滤掉只包含了ID，但没有包含队列的桶

      writeJsonOk(items.asJson)
    }

  private def waitUrlRoute: Route =
    (path("api" / "spider" / "wait" / "list.json") & get & cors(settings)) {
      parameters('page.as[Int] ? 1,
        'limit.as[Int] ? 20,
        'field.as[String] ? "",
        'order.as[String] ? "asc") { (page, limit, field, order) =>
        val cnt = WaitDb.count()
        val data = WaitDb.pageFetchLinks(page, limit).map {
          case (link, fetchTime) =>
            Map(
              "hash" -> link.urlHashHex.asJson,
              "url" -> link.url.asJson,
              "type" -> link.`type`.name.asJson,
              "anchor" -> link.anchor.asJson,
              "depth" -> link.depth.asJson,
              "refer" -> link.refer.getOrElse("").asJson,
              "params" -> link.params.mkString("; ").asJson,
              "fetchTime" -> fetchTime.toString("MM-dd HH:mm:ss").asJson
            ).asJson
        }

        writeJson(jsonTable(cnt, data))
      }
    }

  private def promoteWaitUrls: Route =
    (path("api" / "spider" / "wait" / "promote") & post & cors(settings)) {
      entity(as[String]) {
        body =>
          //Body like: ["hash1","hash2..."]
          import io.circe.parser._
          parse(body) match {
            case Right(json) =>
              val result: Decoder.Result[List[String]] = json.as[List[String]]
              result.getOrElse(List.empty[String]).foreach {
                hash =>
                  WaitDb.promoteTime(hash, DateTime.now())
              }
              writeJsonOk(msg = "操作完成, 请刷新页面查看结果！")
            case Left(e) =>
              writeJsonError(e.getMessage())
          }
      }
    }
}

