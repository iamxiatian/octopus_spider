package xiatian.octopus.expt

import java.text.SimpleDateFormat

import xiatian.octopus.util.{HashUtil, Http}

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Success, Try}

object XianArchive {
  //
  val df = new SimpleDateFormat("yyyy-MM-dd")

  def getPageList(): List[String] = {
    (1 to 36).toList.map {
      page =>
        s"http://www.xadaj.gov.cn/appeal/list.jsp?model_id=2&sq_title=&cur_page=$page"
    }
  }

  /**
    * 解析表格页面，提取出其中的文章链接
    */
  def parseTablePage(pageUrl: String) = {
    val doc = Http.jdoc(pageUrl)

    val items = doc.select("div.wlwzListBox div.wlwzListItem").asScala
    items foreach {
      item =>
        val url = item.select("a").asScala.map(_.attr("abs:href")).head
        val code = item.select("li").asScala.head.text.trim
        parseArticle(url, code)
    }
  }

  def parseArticle(articleUrl: String, code: String) = Try {
    val category = "在线咨询"

    val doc = Http.jdoc(articleUrl)
    val tdItems = doc.select("div.szxxList > table > tbody > tr > td").asScala

    val title = tdItems(1).text.trim
    val person = ""
    val askTime = tdItems(3).text.trim
    val askContent = tdItems(5).text.trim
    val status = "通过"
    val replier = tdItems(7).text.trim
    val replyTime = tdItems(9).text.trim
    val replyContent = tdItems(11).text.trim

    val urlMd5 = HashUtil.md5(articleUrl)

    val consult = ArchiveConsult(code, title, person,
      new java.sql.Timestamp(df.parse(askTime).getTime),
      askContent, status,
      new java.sql.Timestamp(df.parse(replyTime).getTime),
      replier,
      replyContent,
      articleUrl,
      urlMd5,
      0,
      category,
      "Xian"
    )

    Await.result(ArchiveConsultDb.save(consult), Duration.Inf)
  } match {
    case Success(_) =>
    case scala.util.Failure(e) =>
      println(s"$articleUrl")
      e.printStackTrace()
  }

  def collect(): Unit = getPageList foreach parseTablePage
}
