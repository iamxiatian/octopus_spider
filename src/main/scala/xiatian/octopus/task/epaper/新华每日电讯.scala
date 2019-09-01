package xiatian.octopus.task.epaper

import org.joda.time.DateTime
import org.zhinang.protocol.http.UrlResponse
import xiatian.octopus.common.OctopusException
import xiatian.octopus.model.{FetchItem, FetchType}
import xiatian.octopus.parse.{ParseResult, Parser}
import xiatian.octopus.util.{HashUtil, Http}

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * 新华每日电讯: http://mrdx.cn/content/20190208/Page01HO.htm
  */
object 新华每日电讯 extends EPaperTask("新华每日电讯电子报", "新华每日电讯") with Parser {
  override def entryItems: List[FetchItem] = {
    //默认返回最近一月的入口地址, 减去12小时，保证本天的第一次采集时间在12点之后

    (0 to 30).toList.map {
      days =>
        val d = DateTime.now().minusHours(8).minusDays(days)

        //http://mrdx.cn/content/20190208/Page01HO.htm
        val pattern = d.toString("yyyyMMdd")
        val url = s"http://mrdx.cn/content/$pattern/Page01HO.htm"

        FetchItem(
          id,
          url, FetchType.EPaper.Column,
          Option("http://www.xinhuanet.com/mrdx/"),
          None,
          1,
          0
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
      val urls = doc.select("td a.atitle").asScala.filter {
        u => u.attr("href").contains("Artice")
      }
      urls.zipWithIndex.map {
        case (a, idx) =>
          val link = a.attr("abs:href")
          val anchor = a.text().trim

          FetchItem(
            item.taskId,
            link, FetchType.EPaper.Article,
            Option(url), Option(anchor),
            item.depth + 1,
            0,
            Map("column" -> column, "rank" -> (idx + 1).toString)
          )
      }.toList
    }

    def extractColumnUrls(): List[FetchItem] = {
      val columnUrls = doc.select("td a.atitle").asScala.filter {
        u => u.attr("href").contains("Page") && u.text().length > 2
      }
      columnUrls.map {
        a =>
          val link = a.attr("abs:href")
          val columnName = a.text.trim.replaceAll(" ", "")

          FetchItem(
            item.taskId,
            link, FetchType.EPaper.Column,
            Option(url), Option(columnName),
            item.depth + 1,
            0,
            Map.empty
          )
      }.toList
    }


    item.`type` match {
      case FetchType.EPaper.Column =>
        val columnName = doc.select("table#table5 td").asScala
          .headOption
          .map(_.text.replaceAll(" ", ""))
          .getOrElse("Unknown")

        val r = if (url.endsWith("Page01HO.htm")) {
          //第一版，提取文章和列表url
          ParseResult(extractArticleUrls(columnName) ::: extractColumnUrls(), None)
        } else {
          // 非第一版，只提取文章url
          ParseResult(extractArticleUrls(columnName), None)
        }

        if (r.children.isEmpty)
          throw OctopusException(s"栏目未抽取出子链接! ${item.url}")
        else r

      case FetchType.EPaper.Article =>
        val column = item.params("column")
        val rank = item.params("rank").toInt
        val pubDate = "[\\d]{8}".r.findFirstIn(url)
          .map {
            d =>
              d.substring(0, 4) + "-" + d.substring(4, 6) + "-" + d.substring(6)
          }
          .getOrElse("")

        //http://mrdx.cn/content/20190208/Articel04002BB.htm

        val pos = doc.title.lastIndexOf("--")

        val title = if (pos > 0) doc.title.substring(pos + 2).trim
        else doc.title.trim

        val author = ""

        val html = doc.select("span#contenttext").html()

        val text = doc.select("span#contenttext").text.replaceAll(" 　　", "\n")

        val id = HashUtil.md5(url)

        val article = EPaperArticle(
          id,
          url, title, "",
          author,
          pubDate,
          "新华每日电讯",
          column,
          rank,
          text, html)

        ParseResult(List.empty, Some(article))
      case _ =>
        throw OctopusException(s"新华每日电讯任务无法识别的抓取条目: $item")
    }
  }

}
