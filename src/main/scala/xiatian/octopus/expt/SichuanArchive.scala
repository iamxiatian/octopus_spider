package xiatian.octopus.expt

import java.sql.Timestamp
import java.text.SimpleDateFormat

import io.circe.Json
import io.circe.parser.parse
import org.jsoup.Jsoup
import scalaj.http.Http
import xiatian.octopus.util.HashUtil

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * 四川档案信息的抽取，用于论文档案在线咨询的互动数据分析
  *
  * 四川档案解析数据返回的JSON格式：
  *
  * {
  * "approveDate" : "",
  * "approveDesc" : "",
  * "approveTime" : "",
  * "approveUserId" : "",
  * "approveUserName" : "",
  * "clId" : "7207442dc690423a9f56e7ad73cbd0a1",
  * "clName" : "业务咨询",
  * "commentContent" : "已邮件回复。",
  * "commentDate" : "2019-04-04",
  * "commentTime" : "10:10:23",
  * "commentUserId" : "402881124d4aabc3014d4bbf9f31020f",
  * "commentUserName" : "deng",
  * "commitDate" : "2019-04-03",
  * "commitTime" : "18:43:22",
  * "commitUserId" : "5029bbe65504449dbfe0d1932ff08e2d",
  * "commitUserName" : "吕盟",
  * "fbContent" : "......",
  * "fbDate" : "2019-04-04",
  * "fbId" : "66fe64a3c5274dd8a6455bc61403a760",
  * "fbIp" : "",
  * "fbIsRepeat" : "",
  * "fbPreFbId" : "",
  * "fbState" : "4",
  * "fbTitle" : "想找自己的档案",
  * "publishDate" : "2019-04-04",
  * "publishDesc" : "",
  * "publishTime" : "10:10:27",
  * "publishUserId" : "402881124d4aabc3014d4bbf9f31020f",
  * "publishUserName" : "deng",
  * "returnDate" : "",
  * "returnReason" : "",
  * "returnTime" : "",
  * "returnUserId" : "",
  * "returnUserName" : "",
  * "screenDate" : "2019-04-04",
  * "screenResult" : "通过",
  * "screenTime" : "10:09:31",
  * "screenUserId" : "402881124d4aabc3014d4bbf9f31020f",
  * "screenUserName" : "deng",
  * "siteId" : "402881aa5c9f5e71015c9f62507c0022",
  * "siteName" : "四川省档案局（馆）"
  * }
  *
  */
object SichuanArchive {
  val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def parsePage(pageId: Int) = {
    val url = "http://www.scsdaj.gov.cn/scda/default/webfeedback!directorEmailList.action"
    val response = Http(url).postForm(Seq(
      "clId" -> "7207442dc690423a9f56e7ad73cbd0a1",
      "title" -> "",
      "beginPage" -> s"$pageId"))
    val content = response.asString.body
    val root = parse(content).right.get
    val records: Vector[Json] = root.hcursor.downField("result")
      .focus
      .flatMap(_.asArray)
      .getOrElse(Vector.empty)

    records.map {
      r =>
        val code = r.hcursor.get[String]("fbId").toOption.get
        val title = r.hcursor.get[String]("fbTitle").toOption.get
        val person = r.hcursor.get[String]("commitUserName").toOption.get
        val askDate = r.hcursor.get[String]("commitDate").toOption.get
        val time = r.hcursor.get[String]("commitTime").toOption.get
        val askTime = if(time.isEmpty) "00:00:00" else time

        val askContent = r.hcursor.get[String]("fbContent").toOption.get

        val replyContent = r.hcursor.get[String]("commentContent").toOption.get
        val replyDate = r.hcursor.get[String]("commentDate").toOption.get
        val time2 = r.hcursor.get[String]("commentTime").toOption.get
        val replyTime = if(time2.isEmpty) "00:00:00" else time2

        val category = r.hcursor.get[String]("clName").toOption.get
        val status = r.hcursor.get[String]("screenResult").toOption.get

        val url = s"http://www.scsdaj.gov.cn/scda/default/resultListdetail.jsp?fbId=$code"
        val urlMd5 = HashUtil.md5(url)

        ArchiveConsult(
          code, title, person,
          new Timestamp(df.parse(s"$askDate $askTime").getTime),
          Jsoup.parse(askContent).text(),
          status,
          new Timestamp(df.parse(s"$replyDate $replyTime").getTime),
          Jsoup.parse(replyContent).text(),
          url,
          urlMd5,
          category,
          "Sichuan"
        )
    }
  }

//  def main(args: Array[String]): Unit = {
//    //Await.result(ArchiveConsultDb.createSchema, Duration.Inf)
//    (20 to 43) foreach {
//      page =>
//        println(s"process page $page ... ")
//        parsePage(page).foreach {
//          article =>
//            Await.result(ArchiveConsultDb.save(article), Duration.Inf)
//        }
//    }
//  }
}
