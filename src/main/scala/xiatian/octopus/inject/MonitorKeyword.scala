package xiatian.octopus.inject

import java.net.URLEncoder

import xiatian.octopus.util.TryWith

import scala.xml.XML

/**
  * 对于监控关键词的处理：通过分析主流网站的搜索URL，把关键词变为采集任务
  * 的Board对象，进行常规采集。
  *
  * @author Tian Xia
  *         May 14, 2017 10:26
  */
object MonitorKeyword {
  def isAllDigits(x: String) = x forall Character.isDigit

  def injectFromFile(keywordsFile: String): Unit =
    TryWith(
      scala.io.Source.fromFile(keywordsFile, "utf-8")
    ) {
      resource =>
        resource.getLines().foreach(
          line =>
            if (!line.trim.startsWith("#")) {
              val parts = line.split("\t| ", 2)
              if (parts.length == 2 && isAllDigits(parts(0)))
                injectKeywords(parts(0).toInt, parts(1).trim)
            }
        )
    }


  def doc = XML.loadFile("conf/meta-searchers.xml")

  def searchers = (doc \\ "searcher").filter(node => (node \ "@enabled").toString == "true")

  def injectKeywords(id: Int, keyword: String): Unit = {
    println(s"Inject keyword: $id \t $keyword")
    searchers.foreach {
      searcher =>
        val boardText = (searcher \\ "BOARD").toString
        val code = (searcher \\ "Code").text
        val parsedText = boardText
          .replaceAllLiterally("${keyword:utf-8}", URLEncoder.encode(keyword, "utf-8"))
          .replaceAllLiterally("${keyword}", keyword)
          .replaceAllLiterally(s"<Code>${code}</Code>", s"<Code>${code.toInt + id}</Code>")

        println(s"Injected $keyword ... [FAILURE]")
    }
  }

}

