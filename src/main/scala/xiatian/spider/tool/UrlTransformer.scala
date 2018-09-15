package xiatian.spider.tool

import java.net.URLDecoder
import java.util.regex.Pattern

import org.slf4j.LoggerFactory
import org.zhinang.protocol.http.HttpClientAgent
import org.zhinang.protocol.normalizer.URL
import xiatian.common.MyConf

import scala.collection.JavaConverters._
import scala.util.Random

/**
  * URL的处理变换，输入一个URL，变换为转换后的URL，跳转方法有两类：
  *
  * 1. 直接由原来的URL变换过来，例如： http://x.com/url?url=http://abc.com/1.html,
  * 该url后面的参数即为目标url
  * 对应于配置文件中的fetcher.extractingUrls
  *
  * 2. 通过爬虫抓取当前页面，获取跳转后的URL, 如：
  * https://www.baidu.com/link?url=FV3EVojKRxOmuWRXrZL8cc.....
  * 对应于配置文件中的fetcher.jumpingUrls
  *
  * @author Tian Xia
  *         Jun 30, 2017 12:29
  */
object UrlTransformer {
  val log = LoggerFactory.getLogger(UrlTransformer.getClass.getName)

  /**
    * 符合跳转规则的URL正则表达式
    */
  lazy val jumpingUrlRules: List[Pattern] =
    if (MyConf.config.hasPath("fetcher.jumpingUrls")) {
      MyConf.config.getStringList("fetcher.jumpingUrls").asScala
        .map(_.r.pattern)
        .toList
    } else {
      List.empty[Pattern]
    }

  /**
    * paramUrls为(正则表达式,参数名称)的列表，即符合正则表达式的url，会解析该
    * url包含的对应URL参数，以参数值作为实际URL变换结果
    */
  lazy val paramUrls: List[(Pattern, String, String)] =
    if (MyConf.config.hasPath("fetcher.paramUrls")) {
      MyConf.config.getConfigList("fetcher.paramUrls")
        .asScala
        .map(
          item =>
            (
              item.getString("url").asInstanceOf[String].r.pattern,
              item.getString("param").asInstanceOf[String],
              if (item.hasPath("encoding"))
                item.getString("encoding")
              else "UTF-8"
            )
        )
        .toList
    } else {
      List.empty[(Pattern, String, String)]
    }


  val client = new HttpClientAgent(MyConf.zhinangConf)

  /**
    * 把URL根据规则进行变换，如果转换成功，返回Some(transformedUrl)
    * 否则，返回None
    *
    * @param url
    * @return
    */
  def transform(url: String): Option[String] = {
    if (jumpingUrlRules.exists(_.matcher(url).matches())) {
      Some(client.getFollowedUrl(url, 100 + Random.nextInt(200)))
    } else {
      val item = paramUrls.find(pair => pair._1.matcher(url).matches())

      if (item.isEmpty) {
        None
      } else {
        try {
          val target = URLDecoder.decode(
            URL.parse(url).queryParameter(item.get._2),
            item.get._3
          )
          Some(target)
        } catch {
          case e: Exception =>
            log.error(e.toString)
            None
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val testUrl = "https://www.google.com/url?sa=t&rct=j&q=&esrc=s" +
      "&source=newssearch&cd=3&cad=rja&uact=8&ved=0ahUKEwiJnPyi9O" +
      "TUAhWOZj4KHZSHBUoQqQIIKygAMAI&url=https%3A%2F%2Fwww.voachi" +
      "nese.com%2Fa%2Fnews-investigation-launched-into-overseas-inv" +
      "estment-by-companies-allegedly-related-to-top-leaders-201706" +
      "22%2F3911671.html&usg=AFQjCNGZ_0IfBMQdI-KqwvPR7EcxfM-uzg"

    val targetUrl = "https://www.voachinese.com/a/news-investigation-" +
      "launched-into-overseas-investment-by-companies-allegedly-" +
      "related-to-top-leaders-20170622/3911671.html"

    assert(transform(testUrl) == targetUrl)

    val jumpUrl = "https://www.baidu.com/link?url=n7Tas2jw_MEZvYrry" +
      "uqF2LWALH1yHJIl551G2yVcno_&wd=&eqid=8e60662c0003326f00000003" +
      "59561be3"

    val jumpTargetUrl = ""

    assert(jumpUrl == jumpTargetUrl)
  }
}
