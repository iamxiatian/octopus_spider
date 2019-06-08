package xiatian.octopus.expt

import java.text.SimpleDateFormat

import org.jsoup.Jsoup
import scalaj.http.Http
import xiatian.octopus.util.HashUtil

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Success, Try}


/**
  * 贵州档案信息的抽取，用于论文档案在线咨询的互动数据分析
  *
  */
object GuizhouArchive {
  val df = new SimpleDateFormat("yyyy-MM-dd")

  def parsePage(pageNum: Int, numPerPage: Int = 15) = {
    val url = "http://www.gzdaxx.gov.cn/trsapp/appWeb.do?method=queryAppData"
    val response = Http(url).postForm(Seq(
      "isNeedTheme" -> "0",
      "flag" -> "0",
      "appId" -> "302",
      "groupId" -> "364",
      "status" -> "-1",
      "selectField" -> "",
      "sFieldValue" -> "",
      "selectFields" -> "",
      "sFieldValues" -> "",
      "CSSKEY" -> "",
      "pageNum" -> s"$pageNum",
      "numPerPage" -> s"$numPerPage",
      "orderField" -> "crtime",
      "orderDirect" -> "desc"))

    val content = response.asString.body
    val doc = Jsoup.parse(content, url)
    val items = doc.select("table.zf-flex-table-inside tr td.title span").asScala
    items.foreach {
      span =>
        val sqid = span.attr("sqid")
        val articleUrl =  s"http://www.gzdaxx.gov.cn/trsapp/appWeb.do?method=appDataDetail&groupId=364&appId=302&dataId=$sqid"
        parseArticle(articleUrl, sqid)
    }
  }

  def parseArticle(articleUrl: String,
                   code: String) = Try {
    val doc = xiatian.octopus.util.Http.jdoc(articleUrl)

    val items = doc.select("div.gz-xj-tb > table > tbody > tr > td").asScala

    val person = items(0).text.trim
    val title = items(2).text.trim
    val askContent = items(3).text.trim
    val status = "已办理"
    val askTime = items(5).text.trim
    val viewCount = items(6).text.trim.toInt

    val replyTime = items(7).text.trim
    val replier = items(8).text.trim
    val replyContent = items(9).text.trim

    val urlMd5 = HashUtil.md5(articleUrl)

    val consult = ArchiveConsult(code,
      title,
      person,
      new java.sql.Timestamp(df.parse(askTime).getTime),
      askContent, status,
      new java.sql.Timestamp(df.parse(replyTime).getTime),
      replier,
      replyContent,
      articleUrl,
      urlMd5,
      viewCount,
      "信件公开",
      "Guizhou"
    )

    Await.result(ArchiveConsultDb.save(consult), Duration.Inf)
  } match {
    case Success(_) =>
    case scala.util.Failure(e) =>
      println(s"$articleUrl")
      e.printStackTrace()
  }

  def collect() = (1 to 13) foreach {
    p => parsePage(p)
  }

}
