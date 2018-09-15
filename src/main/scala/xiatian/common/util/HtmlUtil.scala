package xiatian.common.util

import java.io.ByteArrayInputStream

import org.jsoup.Jsoup
import org.jsoup.nodes.{Element, Node, TextNode}
import org.slf4j.LoggerFactory
import org.zhinang.util.LinkUtils
import xiatian.spider.tool.SiteAttribute

import scala.collection.JavaConverters._
import scala.collection.mutable

object HtmlUtil {
  val log = LoggerFactory.getLogger("HtmlUtil")

  /**
    * 把html转换为文本，注意，会根据标签的类型插入换行符号
    *
    * @param html
    */
  def text(html: String): String = {
    val body = Jsoup.parse(html).selectFirst("body")
    //递归遍历每一个元素
    val sb = new mutable.StringBuilder()

    def traverse(node: Node): Unit = {
      if (node.isInstanceOf[TextNode]) {
        sb.append(node.asInstanceOf[TextNode].text())
      } else {
        node.childNodes().forEach(traverse(_))

        if (Set("br", "p", "div", "hr", "table", "tr").contains(node.nodeName().toLowerCase())) {
          sb.append("\n")
        }
      }
    }

    if (body != null)
      body.childNodes().forEach(traverse(_))

    //每一行过滤掉首尾的空格，然后去除空行
    sb.split('\n').map(_.trim).filter(_.nonEmpty).mkString("\n\n")
  }

  def parseHrefs(url: String,
                 encoding: String,
                 content: Array[Byte]): Map[String, String] = {

    try {
      val domain = LinkUtils.getDomain(url)
      val cssQuery = SiteAttribute.getContainer(domain, url)

      //      if(SiteAttribute.findLinkByRegex(domain)) {
      //        log.debug("Use regex to extract child links.")
      //        val html = new String(content, encoding)
      //
      //
      if (url.contains("//tieba.baidu.com/")) {
        //@TODO 后面需要修改为通用处理方式
        val html = new String(content, encoding)
        val regex = """<a href="(/p/\d+)" title="([^"]*)"""".r
        val matcher = regex.pattern.matcher(html)

        var children = mutable.MutableList.empty[(String, String)]

        while (matcher.find()) {
          val href = matcher.group(1)
          val title = matcher.group(2)
          children += ((href, title))
        }

        val regex2 = """<a href="(/f\?kw=[^"]*)"""".r
        val matcher2 = regex2.pattern.matcher(html)

        while (matcher2.find()) {
          val href = matcher2.group(1)
          val title = ""
          children += ((href, title))
        }

        children.toMap[String, String].map {
          case (href, title) => (s"https://tieba.baidu.com$href", title)
        }
        //Map.empty[String, String]
      } else if (cssQuery.nonEmpty) {
        val doc = Jsoup.parse(new ByteArrayInputStream(content), encoding, url)
        val elements = doc.select(cssQuery.get)

        if (elements.isEmpty) {
          log.warn(s"No elements are found for specified selector -> ${cssQuery.get}")
          extractHrefs(url, encoding, content)
        } else {
          Map.empty[String, String] ++ elements.asScala.flatMap(extractHrefs)
        }
      } else {
        extractHrefs(url, encoding, content)
      }
    } catch {
      case _: Exception => Map.empty[String, String]
    }
  }

  private def extractHrefs(url: String,
                           encoding: String,
                           content: Array[Byte]): Map[String, String] = {
    val doc = Jsoup.parse(new ByteArrayInputStream(content), encoding, url)
    extractHrefs(doc)
  }

  def extractHrefs(root: Element) =
    root.getElementsByTag("a").asScala
      .map(
        e => (e.attr("abs:href"), e.text())
      )
      .groupBy(_._1)
      .map {
        case (k, v) => {
          v.maxBy(_._2.length)
        }
      } //按照URL分组，根据anchor长度，保留最长的anchor
      .filter { case (u: String, anchor) => u.startsWith("http") }

}
