package xiatian.octopus.expt

import java.text.SimpleDateFormat

import xiatian.octopus.util.{HashUtil, Http}

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Success, Try}

/**
  * 天津档案信息的抽取，用于论文档案在线咨询的互动数据分析
  */
object TianjinArchive {
  def getPageList(): List[(String, String)] = {
    (1 to 47).toList.map {
      page =>
        val url = s"http://www.tjdag.gov.cn/eportal/ui?currentPage=$page" +
          s"&moduleId=22d56c5648e941a3b5f7e8afa35b29a6&pageId=300740"
        (url, "网上咨询")
    }
  }

  def getAllPageList(): List[(String, String)] = {
    val liuyan = (1 to 39).toList.map {
      page =>
        "http://www.tjdag.gov" +
          s".cn/eportal/ui?currentPage=${page}&moduleId=" +
          "0cbec7ecc7c1453e8faa1b6e0478ead3&pageId=300746"
    }.map((_, "公众留言"))

    val zixun = (1 to 47).toList.map {
      page =>
        s"http://www.tjdag.gov.cn/eportal/ui?currentPage=${page}&moduleId" +
          s"=22d56c5648e941a3b5f7e8afa35b29a6&pageId=300740"
    }.map((_, "网上咨询"))

    //    结果反馈和网上咨询与公众留言的内容相同
    //    val jieguo = (1 to 86).toList.map {
    //      page =>
    //        s"http://www.tjdag.gov" +
    //          s".cn/eportal/ui?currentPage=${page}&moduleId" +
    //          s"=3ad9d3c5b39e4fbd88174631471f39f1&pageId=300752"
    //    }.map((_, "结果反馈"))

    zixun ::: liuyan
  }

  def parsePage(pageUrl: String, category: String) = {
    val doc = Http.jdoc(pageUrl)
    val articleUrls = doc.select("span.easysite-theme a")
      .asScala.map(_.attr("abs:href"))
    articleUrls.foreach(parseArticle(_, category))
  }

  def parseArticle(articleUrl: String, category: String) = Try {
    val doc = Http.jdoc(articleUrl)
    val codeText = doc.select("div.easysite-detail-tile p span")
      .text.trim

    val title = doc.select("div.easysite-detail-tile h3").text.trim

    val sections = doc.select("div.easysite-detail-content div.easysite-detail-section").asScala

    val person = sections(0).select("div.easysite-detail-info p").text.trim
    val askTime = sections(1).select("div.easysite-detail-info p").text.trim
    val askContent = sections(2).select("div.easysite-detail-info p").text.trim
    val status = sections(3).select("div.easysite-detail-info p").text.trim
    val replyTime = sections(4).select("div.easysite-detail-info p").text.trim
    val replyContent = sections(5).select("div.easysite-detail-info p").text.trim
    val urlMd5 = HashUtil.md5(articleUrl)


    val code = if (codeText == "编号") {
      //说明编号为空, 自己补上默认的编号
      s"${askTime.replaceAll(" ", "").replaceAll(":", "").replaceAll("-", "")}00000"
    } else {
      codeText.substring(3)
    }

    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val consult = ArchiveConsult(code, title, person,
      new java.sql.Timestamp(format.parse(askTime).getTime),
      askContent, status,
      new java.sql.Timestamp(format.parse(replyTime).getTime),
      "",
      replyContent,
      articleUrl,
      urlMd5,
      0,
      category,
      "Tianjin"
    )

    Await.result(ArchiveConsultDb.save(consult), Duration.Inf)
  } match {
    case Success(_) =>
    case scala.util.Failure(e) =>
      println(s"$articleUrl")
      e.printStackTrace()
  }

//  def main(args: Array[String]): Unit = {
//    Await.result(ArchiveConsultDb.createSchema, Duration.Inf)
//    getAllPageList foreach {
//      pair =>
//        parsePage(pair._1, pair._2)
//    }
//  }
}
