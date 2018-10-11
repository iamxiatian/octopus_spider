package xiatian.spider.tool

import java.io.{ByteArrayInputStream, File}

import com.google.common.io.Files
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.slf4j.LoggerFactory
import org.zhinang.protocol.http.{HttpClientAgent, UrlResponse}
import xiatian.common.MyConf.zhinangConf
import xiatian.spider.actor.ProxyIp
import xiatian.spider.model.FetchLink

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

  def get(link: FetchLink,
          proxyHolder: Option[ProxyIp] = None,
          contentType: String = "text/html"): UrlResponse =
    get(link.url, link.refer.getOrElse(""), proxyHolder, contentType)

  def get(url: String): UrlResponse = get(url, "", None, "text/html")

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
          log.info(s"Use proxy ${proxyIp} to fetch url ${url}")
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
}
