package xiatian.octopus.actor.fetcher

import java.net.URLDecoder
import java.util.regex.Pattern

import org.slf4j.LoggerFactory
import org.zhinang.protocol.http.HttpClientAgent
import org.zhinang.protocol.normalizer.URL
import xiatian.octopus.common.{Logging, MyConf}

import scala.collection.JavaConverters._
import scala.util.{Random, Success, Try}

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
object TransformUrl {
  val log = LoggerFactory.getLogger(TransformUrl.getClass.getName)

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
  lazy val paramUrls: List[ParamUrl] =
    if (MyConf.config.hasPath("fetcher.paramUrls")) {
      MyConf.config.getConfigList("fetcher.paramUrls")
        .asScala
        .map {
          item =>
            import MyConf.ConfImplicits._

            val pattern = item.getString("url").r.pattern
            val param = item.getStringOption("param")
            val regex = item.getStringOption("regex")
            val encoding = item.getString("encoding", "UTF-8")
            if (param.nonEmpty) {
              ParamUrl(pattern, QueryParam(param.get, encoding))
            } else if (regex.nonEmpty) {
              ParamUrl(pattern, RegexParam(regex.get, encoding))
            } else {
              log.error("must specify param or regex!")
              ParamUrl(pattern, EmptyParam())
            }
        }
        .toList
    } else {
      List.empty[ParamUrl]
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
      paramUrls.find(_.matches(url)).flatMap(_.param.extract(url))
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

    assert(transform(testUrl).get == targetUrl)

    //    val jumpUrl = "https://www.baidu.com/link?url=n7Tas2jw_MEZvYrry" +
    //      "uqF2LWALH1yHJIl551G2yVcno_&wd=&eqid=8e60662c0003326f00000003" +
    //      "59561be3"
    //
    //    val jumpTargetUrl = ""
    //
    //    assert(jumpUrl == jumpTargetUrl)
    val yahooUrl = "https://r.search.yahoo.com/_ylt=Awr9DuJ7NnFchWEAAa5XNyoA;_ylu=X3oDMTEyc3JpNTdmBGNvbG8DZ3ExBHBvcwMzBHZ0aWQDQjI5NDRfMQRzZWMDc3I-/RV=2/RE=1550952187/RO=10/RU=https%3a%2f%2fwww.lonelyplanet.com%2fchina/RK=2/RS=3KE0v7kolP5qhPHUtDfUXRr_WTc-"
    assert(transform(testUrl).get == "https://www.lonelyplanet.com/china")
  }
}

sealed trait Param extends Logging {
  def value: String

  def encoding: String

  /**
    * 从url中抽取出最终可以抓取的目标url
    *
    * @param url
    * @return
    */
  def extract(url: String): Option[String] = None

  /**
    * 对编码后的url进行解码，例如：https%3a%2f%2fwww.lonelyplanet.com%2fchina
    * 解码为：https://www.lonelyplanet.com/china
    *
    * @param url
    * @param encoding
    * @return
    */
  def decode(url: String, encoding: String): Option[String] = Try {
    URLDecoder.decode(url, encoding)
  } match {
    case Success(target) => Option(target)
    case scala.util.Failure(e) =>
      LOG.error(e.toString)
      None
  }
}

//URL里面的参数，如Google示例中的url参数
private[rule] case class QueryParam(value: String, encoding: String) extends
  Param {
  override def extract(url: String): Option[String] = Try {
    URLDecoder.decode(URL.parse(url).queryParameter(value), encoding)
  } match {
    case Success(target) => Option(target)
    case scala.util.Failure(e) =>
      LOG.error(e.toString)
      None
  }
}

//通过正则表达式获取url里面的内容
private[rule] case class RegexParam(value: String, encoding: String) extends Param {
  override def extract(url: String): Option[String] = {
    val matcher = value.r.pattern.matcher(url)
    if (matcher.find()) {
      if (matcher.groupCount() == 1) {
        decode(matcher.group(1), encoding)
      } else {
        LOG.error(s"$value has ${matcher.groupCount()}, we need only 1")
        None
      }
    } else None
  }
}

private[rule] case class EmptyParam(value: String = "", encoding: String = "")
  extends Param

/**
  * 参数类型的URL，URL中包含了真正的目标地址，如：
  * https://www.google.com/url?sa=t&rct=j&q=&esrc=s&source=newssearch&cd=3&cad=rja&uact=8&ved=0ahUKEwiJnPyi9OTUAhWOZj4KHZSHBUoQqQIIKygAMAI&url=https%3A%2F%2Fwww.voachinese.com%2Fa%2Fnews-investigation-launched-into-overseas-investment-by-companies-allegedly-related-to-top-leaders-20170622%2F3911671.html&usg=AFQjCNGZ_0IfBMQdI-KqwvPR7EcxfM-uzg
  * 里面的url参数。
  *
  * 或者：
  *
  * # yahoo: https://r.search.yahoo.com/_ylt=Awr9DuJ7NnFchWEAAa5XNyoA;_ylu=X3oDMTEyc3JpNTdmBGNvbG8DZ3ExBHBvcwMzBHZ0aWQDQjI5NDRfMQRzZWMDc3I-/RV=2/RE=1550952187/RO=10/RU=https%3a%2f%2fwww.lonelyplanet.com%2fchina/RK=2/RS=3KE0v7kolP5qhPHUtDfUXRr_WTc-
  * 需要获取“/RU=”和“/RK”之间的内容，此时param改为regex
  *
  */
case class ParamUrl(pattern: Pattern, param: Param) {
  def matches(url: String): Boolean = {
    pattern.matcher(url).matches()
  }
}