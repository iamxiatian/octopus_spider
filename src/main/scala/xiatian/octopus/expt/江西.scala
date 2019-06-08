package xiatian.octopus.expt

import java.text.SimpleDateFormat

import xiatian.octopus.util.{HashUtil, Http}

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Success, Try}

object JiangxiArchive {
  //
  val df = new SimpleDateFormat("yyyy-MM-dd")

  def getPageList(): List[(String, String)] = {
    val jianyan = (1 to 2).toList.map {
      p =>
        (s"http://www.jxdaj.gov.cn/" +
          s"id_JYXC2015121011215903/page_$p/ReferSearch.shtml",
          "建言献策")
    }


    val dayin = (1 to 16).toList.map {
      p =>
        (s"http://www.jxdaj.gov.cn/" +
          s"id_ZXDY2015121011433959/page_$p/ReferSearch.shtml",
          "在线答疑")
    }

    jianyan
  }

  /**
    * 解析表格页面，提取出其中的文章链接
    */
  def parseTablePage(pageUrl: String, category: String) = {
    val doc = Http.jdoc(pageUrl)
    val items = doc.select("div.news-list > table:eq(1) > tbody > tr > td:eq(1) a").asScala

    items.map(e => e.attr("abs:href")).foreach {
      articleUrl =>
        parseArticle(articleUrl, category)
    }
  }

  def parseArticle(articleUrl: String, category: String) = Try {
    //http://www.jxdaj.gov.cn/id_2c908198677d64dd0167811b5ca50955/ReferContent.shtml
    val code = articleUrl.substring(articleUrl.indexOf("id_") + 3, articleUrl.indexOf("/ReferContent"))
    val doc = Http.jdoc(articleUrl)
    val tdItems = doc.select("div.news-list > table:gt(0) > tbody > tr > td").asScala

    val title = tdItems(6).text.trim
    val person = tdItems(2).text.trim
    val email = tdItems(4).text.trim
    val askTime = tdItems(8).text.trim
    val askContent = tdItems(10).text.trim
    val status = "已办结"

    val replier = ""
    val replyTime = tdItems(13).text.trim
    val replyContent = tdItems(15).text.trim

    val urlMd5 = HashUtil.md5(articleUrl)

    val consult = ArchiveConsult(code, title,
      s"$person/$email",
      new java.sql.Timestamp(df.parse(askTime).getTime),
      askContent, status,
      new java.sql.Timestamp(df.parse(replyTime).getTime),
      replier,
      replyContent,
      articleUrl,
      urlMd5,
      0,
      category,
      "Jiangxi"
    )

    Await.result(ArchiveConsultDb.save(consult), Duration.Inf)
  } match {
    case Success(_) =>
    case scala.util.Failure(e) =>
      println(s"$articleUrl")
      e.printStackTrace()
  }

  def collect(): Unit = getPageList().foreach {
    case (pageUrl, category) =>
      parseTablePage(pageUrl, category)
  }
}
