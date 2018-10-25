package xiatian.octopus.util

import java.io.{ByteArrayInputStream, File}

import com.google.common.io.Files
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.slf4j.LoggerFactory
import org.zhinang.protocol.http.{HttpClientAgent, UrlResponse}
import xiatian.octopus.actor.ProxyIp
import xiatian.octopus.common.MyConf.zhinangConf
import xiatian.octopus.model.FetchLink

import scala.collection.mutable
import scala.concurrent.Future

/**
  * 获取网页内容的HTTP封装对象, 需要缓存支持代理的HttpClientAgent，避免每次都实例化一个
  * HttpClientAgent对象，并尝试多次连接代理服务器，导致Connection Refused.
  */
object Http {
  val LOG = LoggerFactory.getLogger(this.getClass)

  /**
    * 主键为代理的地址，Value为HttpClientAgent和加入的时间构成的二元组
    * 记录时间的目的是: Remo    */
  val clients = mutable.Map.empty[String, (HttpClientAgent, Long)]

  val defaultClient = new HttpClientAgent(zhinangConf)

  def get(url: String): UrlResponse = get(url, "", None, "text/html")

  /**
    * 通过代理获取网页信息
    *
    * @param url
    * @param proxyAddress "host:ip"的形式，例如："101.204.175.122"
    * @return
    */
  def getFromProxy(url: String, proxyAddress: String): UrlResponse = {
    val (host, port) = proxyAddress.split(":").toList match {
      case a :: b :: _ => (a, b.toInt)
      case _ => ("", 0)
    }

    get(url, "", Option(ProxyIp(host, port, System.currentTimeMillis() + 10000)), "text/html")
  }

  def get(url: String,
          refer: String,
          proxyHolder: Option[ProxyIp],
          contentType: String): UrlResponse = {
    if (clients.size > 100) {
      val keys = clients.keys
      keys.foreach {
        key =>
          clients.get(key).map {
            case (client, time) =>
              //超过1小时，则删除该缓存中的条目
              if (time + 3600000 < System.currentTimeMillis()) {
                clients.remove(key)
              }
          }
      }
    }

    val client = proxyHolder match {
      case Some(proxyIp) =>
        if (proxyIp.expired()) {
          //移除缓存中的HttpClientAgent
          clients.remove(proxyIp.address)
          //new HttpClientAgent(zhinangConf)
          defaultClient
        } else synchronized {
          LOG.info(s"Use proxy ${proxyIp} to fetch url ${url}")
          if (clients.contains(proxyIp.address)) clients(proxyIp.address)._1
          else {
            val c = new HttpClientAgent(zhinangConf, proxyIp.host, proxyIp.port)
            clients.put(proxyIp.address, (c, System.currentTimeMillis()))
            c
          }
        }
      case None =>
        //new HttpClientAgent(zhinangConf)
        defaultClient
    }

    //只有text/html类型的网页才会继续提取内容，填充到response对象中
    client.execute(
      url,
      refer,
      contentType)
  }

  /**
    * 保存图片链接到一个图片文件中
    */
  def saveImage(url: String,
                imgFile: File,
                refer: String = "",
                proxyHolder: Option[ProxyIp] = None,
                contentType: String = "image"): Future[Boolean] = Future.successful {
    val response = get(url, refer, proxyHolder, contentType)
    if (response.getCode == 200) {
      if (!imgFile.getParentFile.exists()) {
        imgFile.getParentFile.mkdirs()
      }
      try {
        Files.write(response.getContent, imgFile)
        true
      } catch {
        case e: Exception =>
          LOG.error(e.getMessage)
          false
      }
    } else {
      LOG.error(s"save image error for $url (code = ${response.getCode})")
      false
    }
  }

  /**
    * 获取URL的文件类型，即不带点的文件后缀名。例如:http://www.test.com/123.jpg?id=1，
    * 返回 jpg
    *
    * @param url
    * @param defaultValue 如果url不存在扩展名时，返回该值
    * @return 不带点和参数信息的文件后缀名，如html, jpg等
    */
  def getFileType(url: String, defaultValue: String): String =
    if (!url.contains("."))
      defaultValue
    else {
      val ext = url.substring(url.lastIndexOf(".") + 1)
      val paramPosition = ext.indexOf("?")

      if (paramPosition > 0)
        ext.substring(0, paramPosition)
      else
        ext
    }

  /**
    * 根据FetchLink转换为JSoup的Document对象
    *
    * @param link
    * @param proxyHolder
    * @param contentType
    * @return
    */
  def jdoc(link: FetchLink,
           proxyHolder: Option[ProxyIp] = None,
           contentType: String = "text/html"): Document = {
    val response = get(link, proxyHolder, contentType)
    Jsoup.parse(new ByteArrayInputStream(response.getContent),
      response.getEncoding,
      link.url
    )
  }

  def get(link: FetchLink,
          proxyHolder: Option[ProxyIp] = None,
          contentType: String = "text/html"): UrlResponse =
    get(link.url, link.refer.getOrElse(""), proxyHolder, contentType)
}
