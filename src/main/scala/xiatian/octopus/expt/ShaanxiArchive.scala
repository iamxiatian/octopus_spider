package xiatian.octopus.expt

import java.text.SimpleDateFormat

import xiatian.octopus.util.{HashUtil, Http}

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Success, Try}

/**
  * 陕西档案信息的抽取，用于论文档案在线咨询的互动数据分析
  */
object ShaanxiArchive {
  val df = new SimpleDateFormat("yyyy-MM-dd")

  def getPageList(): List[(String, String)] = {
    val xinxiang = (1 to 9).toList.map {
      page =>
        val url = s"http://daj.shaanxi.gov.cn/MessageList.aspx?tid=1&p=$page"
        (url, "局长信箱")
    }

    val zixun = (1 to 7).toList.map {
      page =>
        val url = s"http://daj.shaanxi.gov.cn/MessageList.aspx?tid=2&p=$page"
        (url, "业务咨询")
    }

    xinxiang ::: zixun
  }

  def parsePage(pageUrl: String, category: String) = {
    val doc = Http.jdoc(pageUrl)
    val articleUrls = doc.select("table.tablelist tbody td a")
      .asScala.map(_.attr("abs:href"))
    articleUrls.foreach(parseArticle(_, category))
  }

  def parseArticle(articleUrl: String, category: String) = Try {
    val doc = Http.jdoc(articleUrl)
    val tdItems = doc.select("table.tbmain table.tableView td").asScala

    val title = tdItems(3).text.trim
    val person = tdItems(1).text.trim
    val askTime = tdItems(5).text.trim
    val askContent = tdItems(7).text.trim
    val status = "通过"
    val replyTime = tdItems(11).text.trim

    val replyContent = tdItems(13).text.trim
    val urlMd5 = HashUtil.md5(articleUrl)

    val pos = articleUrl.indexOf("id=")
    val code = articleUrl.substring(pos + 3)

    val consult = ArchiveConsult(code, title, person,
      new java.sql.Timestamp(df.parse(askTime).getTime),
      askContent, status,
      new java.sql.Timestamp(df.parse(replyTime).getTime),
      "",
      replyContent,
      articleUrl,
      urlMd5,
      0,
      category,
      "Shaanxi"
    )

    Await.result(ArchiveConsultDb.save(consult), Duration.Inf)
  } match {
    case Success(_) =>
    case scala.util.Failure(e) =>
      println(s"$articleUrl")
      e.printStackTrace()
  }
//
//  def main(args: Array[String]): Unit = {
//
//    parseArticle("http://daj.shaanxi.gov.cn/MessageView.aspx?id=1041", "")
//
//    //    ShaanxiArchive.getPageList.take(1) foreach {
//    //      pair =>
//    //        parsePage(pair._1, pair._2)
//    //    }
//  }
}
