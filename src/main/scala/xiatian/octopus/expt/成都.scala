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
  * 成都档案信息的抽取，用于论文档案在线咨询的互动数据分析
  *
  */
object ChengduArchive {
  val df = new SimpleDateFormat("yyyy-MM-dd")

  def parsePage(pageId: Int, numPerPage: Int = 10) = {
    val url = "http://cdarchive.chengdu.gov.cn/trsapp/appWeb.do?method=queryAppDataByFields"
    val response = Http(url).postForm(Seq(
      "appId" -> "21306",
      "groupId" -> "19098",
      "showStatus" -> "1",
      "pageNum" -> s"$pageId",
      "field" -> "CRTIME,NAME,TITLE",
      "status" -> "-1",
      "selectFields" -> "",
      "sFieldValue" -> "",
      "selectFields" -> "",
      "sFieldValues" -> "",
      "CSSKEY" -> "CT20170929103250644",
      "isHome" -> "0",
      "numPerPage" -> s"$numPerPage",
      "orderField" -> "crtime",
      "orderDirect" -> "desc"))

    val content = response.asString.body
    val doc = Jsoup.parse(content, url)
    val rows = doc.select("table.zf-flex-table-inside > tbody > tr")
      .asScala.drop(1) //remove header
    rows.zipWithIndex foreach {
      case (row, idx) =>
        val items = row.select("> td").asScala
        val askTime = items(0).text.trim
        val person = items(1).text.trim
        val code = items(2).select("span").attr("sqid").trim

        val articleUrl = s"http://cdarchive.chengdu.gov.cn/trsapp/appWeb.do?method=appDataDetail&groupId=19098&appId=21306&dataId=$code"

        parseArticle(articleUrl, code, person, askTime)
    }
  }

  def parseArticle(articleUrl: String,
                   code: String,
                   person: String,
                   askTime: String) = Try {
    val doc = xiatian.octopus.util.Http.jdoc(articleUrl)

    val items = doc.select("div.gz-xj-tb > table > tbody > tr > td").asScala

    val title = items(0).text.trim

    val askContent = items(1).text.trim
    val status = items(4).text.trim
    val replyTime = items(2).text.trim
    val replyContent = items(3).text.trim

    val urlMd5 = HashUtil.md5(articleUrl)

    val consult = ArchiveConsult(code,
      title,
      person,
      new java.sql.Timestamp(df.parse(askTime).getTime),
      askContent, status,
      new java.sql.Timestamp(df.parse(replyTime).getTime),
      "",
      replyContent,
      articleUrl,
      urlMd5,
      0,
      "局长信箱",
      "Chengdu"
    )

    Await.result(ArchiveConsultDb.save(consult), Duration.Inf)
  } match {
    case Success(_) =>
    case scala.util.Failure(e) =>
      println(s"$articleUrl")
      e.printStackTrace()
  }

  def collect() = (1 to 22) foreach {
    p => parsePage(p)
  }

}
