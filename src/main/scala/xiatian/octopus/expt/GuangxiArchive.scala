package xiatian.octopus.expt

import java.text.SimpleDateFormat

import xiatian.octopus.util.{HashUtil, Http}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}


/**
  * 广西档案信息的抽取，用于论文档案在线咨询的互动数据分析
  */
object GuangxiArchive {
  val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  val dateLength = "yyyy-MM-dd HH:mm:ss".length

  def getPageList(): List[(String, String)] = {
    (1 to 17).toList.map {
      page =>
        val url = s"http://weixin.gxdaj.com.cn/index" +
          s".php?g=Wap&m=Reply&a=index&token=pkjcox1435071545&wecha_id" +
          s"=odFT0jpP8R3wEZA&p=$page"
        (url, "网上咨询")
    }
  }

  def isDate(s: String): Boolean = {
    s.forall(p =>
      p >= '0' && p <= '9' || p == ':' || p == '-' || p == ' '
    )
  }

  def parseList(url: String, category: String) = {
    println(s"process $url")
    val doc = Http.jdoc(url)
    val items = doc.select("li.green:has(h3)").asScala

    items.foreach {
      item =>
        val nameAndTime = item.select("h3").text
        val splitPosition = nameAndTime.length - dateLength
        val person = nameAndTime.substring(0, splitPosition)
        val askTime = nameAndTime.substring(splitPosition)

        val title = "无"
        val askContent = item.select("dt.hfinfo").text
        val status = "通过"

        val rcRaw = item.select("dl:eq(4)").text.trim
        val rc = if (rcRaw.startsWith("回复：")) rcRaw.substring(3) else rcRaw

        val pos = rc.length - dateLength

        val (replyContent, replyTime) = if (pos > 0) {
          val time = rc.substring(pos)
          if (isDate(time))
            (rc.substring(0, pos).trim, time)
          else
            (rc, askTime)
        } else (rc, askTime)

        val df2 = new SimpleDateFormat("yyyyMMddHHmmss")
        val code = df2.format(df.parse(askTime))

        val articleUrl = s"$url#$code"
        val urlMd5 = HashUtil.md5(articleUrl)

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
          "Guangxi"
        )

        val f = ArchiveConsultDb.exists(code, "Guangxi").flatMap {
          case true =>
            println(s"$code has already exist in Guangxi dataset.")
            Future.successful(0)
          case false =>
            ArchiveConsultDb.save(consult)
        }

        Await.result(f, Duration.Inf)
    }
  }

  //
  //  def main(args: Array[String]): Unit = {
  //    GuangxiArchive.getPageList().take(1).foreach {
  //      pair =>
  //        GuangxiArchive.parseList(pair._1, pair._2)
  //    }
  //  }
}
