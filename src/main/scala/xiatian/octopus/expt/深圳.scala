package xiatian.octopus.expt

import java.text.SimpleDateFormat

import org.jsoup.Jsoup
import xiatian.octopus.util.{CookieHttp, HashUtil}

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Success, Try}

object ShenzhenArchive {
  //
  val df = new SimpleDateFormat("yyyy.MM.dd")
  val http = new CookieHttp("61.144.227.212", "/was5")

  def getPageList(): List[String] = {
    (1 to 32).toList.map {
      page =>
        s"http://61.144.227.212/was5/web/search?channelid=260234&searchword" +
          s"=accdept%3D51&page=$page"
    }
  }

  /**
    * 解析表格页面，提取出其中的文章链接
    */
  def parseTablePage(listUrl: String) = {
    val response = http.get(listUrl)
    val html = new String(response.content.get, "utf-8")

    val doc = Jsoup.parse(html, listUrl)

    val items = doc.select("td > table").asScala.drop(2).dropRight(1)

    items foreach {
      item =>
        val url = item.select("a").asScala.map(_.attr("abs:href")).head
        parseArticle(url, response.sessionId)
    }
  }

  def parseArticle(articleUrl: String,
                   sessionId: Option[String]) =
    Try {
      val response = http.get(articleUrl, sessionId)
      val html = new String(response.content.get, "utf-8")
      val doc = Jsoup.parse(html, articleUrl)

      val tdItems = doc.select("table.publicTable > tbody > tr > td").asScala

      val title = tdItems(3).text.trim
      val person = ""
      val askTime = tdItems(5).text.trim
      val askContent = tdItems(11).text.trim
      val category = tdItems(7).text.trim
      val status = tdItems(9).text.trim

      //深圳市档案局 2015.12.15
      val s = tdItems(15).text.trim
      val pos = s.length - 10
      val replier = s.substring(0, pos).trim
      val replyTime = s.substring(pos).trim

      val replyContent = tdItems(13).text.trim


      val code = {
        val start = articleUrl.indexOf("documentid=")
        val s = articleUrl.substring(start + "documentid=".length)
        val end = s.indexOf("&")
        s.substring(0, end)
      }
      val normalizedUrl = s"http://61.144.227.212/was5/web/zxts_details.jsp?documentid=$code&channelid=291725"
      val urlMd5 = HashUtil.md5(normalizedUrl)

      val consult = ArchiveConsult(code, title, person,
        new java.sql.Timestamp(df.parse(askTime).getTime),
        askContent, status,
        new java.sql.Timestamp(df.parse(replyTime).getTime),
        replier,
        replyContent,
        normalizedUrl,
        urlMd5,
        0,
        category,
        "Shenzhen"
      )

      Await.result(ArchiveConsultDb.save(consult), Duration.Inf)
    } match {
      case Success(_) =>
      case scala.util.Failure(e) =>
        println(s"$articleUrl")
        e.printStackTrace()
    }

  def collect(): Unit = getPageList foreach parseTablePage


  def main(args: Array[String]): Unit = {
    val listUrl = "http://61.144.227.212/was5/web/search?page=9" +
      "&channelid=260234&searchword=accdept%3D51&keyword=accdept%3D51&perpage=15&outlinepage=10"
    val http = new CookieHttp("61.144.227.212", "/was5")
    val response = http.get(listUrl)
    val html = new String(response.content.get, "utf-8")

    val doc = Jsoup.parse(html, listUrl)

    val items = doc.select("td > table").asScala.drop(2).dropRight(1)

    val urls = items map {
      item =>
        item.select("a").asScala.map(_.attr("abs:href")).head
    }

    println(response.sessionId)

    val r = http.get(urls(0), response.sessionId)
    println(new String(r.content.get, "utf-8"))
  }
}
