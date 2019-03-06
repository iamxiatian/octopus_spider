package xiatian.octopus.task.epaper

import org.joda.time.DateTime
import org.zhinang.protocol.http.UrlResponse
import xiatian.octopus.common.OctopusException
import xiatian.octopus.model.{FetchItem, FetchType}
import xiatian.octopus.parse.{ParseResult, Parser}
import xiatian.octopus.util.{HashUtil, Http}

import scala.collection.JavaConverters._
import scala.util.Try

object 人民日报 extends EPaperTask("人民日报电子报", "人民日报电子报") with Parser {
  override def entryItems: List[FetchItem] = {
    //默认返回最近一月的入口地址, 减去12小时，保证本天的第一次采集时间在12点之后

    (0 to 2).toList.map {
      days =>
        val d = DateTime.now().minusHours(12).minusDays(days)

        //http://paper.people.com.cn/rmrb/html/2019-03/01/nbs.D110000renmrb_01.htm
        val pattern = d.toString("yyyy-MM/dd")
        val url = s"http://paper.people.com.cn/rmrb/html/$pattern/" +
          s"nbs.D110000renmrb_01.htm"

        FetchItem(url, FetchType.EPaper.Column,
          Option("http://paper.people.com.cn/"),
          None,
          1,
          0,
          id
        )
    }
  }

  /**
    * 该任务对应的解析器, 利用该解析器可以对抓取条目进行解析，获取其中的内容
    *
    * @return
    */
  override def parser: Option[Parser] = Some(this)

  override def parse(item: FetchItem, response: UrlResponse): Try[ParseResult] = Try {
    val url = item.value
    val doc = Http.jdoc(response)

    def extractArticleUrls(column: String): List[FetchItem] = {
      val articleUrls = doc.select("div#titleList a").asScala
      articleUrls.zipWithIndex.map {
        case (a, idx) =>
          val link = a.attr("abs:href")
          val text = a.select("script").html
          val first = text.indexOf("\"")
          val last = text.lastIndexOf("\"")

          val anchor = if (first > 0 && last > first)
            text.substring(first + 1, last).trim
          else text

          FetchItem(link, FetchType.EPaper.Article,
            Option(url), Option(anchor),
            item.depth + 1,
            0,
            item.taskId,
            Map("column" -> column, "rank" -> (idx + 1).toString)
          )
      }.toList
    }

    def extractColumnUrls(): List[FetchItem] = {
      val columnUrls = doc.select("div#pageList a")
        .asScala.filter(_.attr("href").contains(".htm"))
      columnUrls.map {
        a =>
          val link = a.attr("abs:href")
          val columnName = a.text.trim

          FetchItem(link, FetchType.EPaper.Column,
            Option(url), Option(columnName),
            item.depth + 1,
            0,
            item.taskId,
            Map.empty
          )
      }.toList
    }


    item.`type` match {
      case FetchType.EPaper.Column =>
        val columnName = doc.select("div.list_l div.l_t").text.trim

        if (url.endsWith("nbs.D110000renmrb_01.htm")) {
          //第一版，提取文章和列表url
          ParseResult(extractArticleUrls(columnName) ::: extractColumnUrls(), None)
        } else {
          // 非第一版，只提取文章url
          ParseResult(extractArticleUrls(columnName), None)
        }
      case FetchType.EPaper.Article =>
        val column = item.params("column")
        val rank = item.params("rank").toInt
        val pubDate = "[\\d]{4}\\-[\\d]{2}/[\\d]{2}".r.findFirstIn(url)
          .map(_.replace("/", "-"))
          .getOrElse("")

        //val title =
        val title = doc.select("div.text_c").asScala
          .flatMap(_.children().asScala) //去除所有子节点
          .filter {
          n =>
            val name = n.nodeName
            name == "h1" || name == "h2" //保留所有的h1, h2标记
        }
          .map(_.text().trim)
          .filter(_.nonEmpty)
          .mkString(" ")

        val author = doc.select("div.text_c").asScala
          .flatMap(_.children().asScala) //去除所有子节点
          .filter(_.nodeName() == "h4")
          .map(_.text.trim.replaceAll(" ", ""))
          .mkString(" ")

        val html = doc.select("div.text_c").html
        val text = doc.select("div.text_c div.c_c p").asScala.map(_.text).mkString("\n")
        val id = HashUtil.md5(url)

        val article = EPaperArticle(
          id,
          url, title, author,
          pubDate, column, rank,
          text, html)

        ParseResult(List.empty, Some(article))
      case _ =>
        throw OctopusException(s"人民日报任务无法识别的抓取条目: $item")
    }
  }

}
