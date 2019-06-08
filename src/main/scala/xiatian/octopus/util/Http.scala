package xiatian.octopus.util

import java.io.{ByteArrayInputStream, File}

import com.google.common.io.Files
import org.apache.http.client.methods.CloseableHttpResponse
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.zhinang.protocol.http.{HttpClientAgent, UrlResponse}
import xiatian.octopus.actor.ProxyIp
import xiatian.octopus.common.Logging
import xiatian.octopus.common.MyConf.zhinangConf
import xiatian.octopus.model.FetchItem

import scala.concurrent.Future

/**
  * 获取网页内容的HTTP封装对象, 需要缓存支持代理的HttpClientAgent，避免每次都实例化一个
  * HttpClientAgent对象，并尝试多次连接代理服务器，导致Connection Refused.
  */
object Http extends Logging {

  val defaultClient = new HttpClientAgent(zhinangConf)

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

    get(url, "",
      Option(ProxyIp(host, port, System.currentTimeMillis() + 10000)),
      Option("text/html"))
  }

  def get(link: FetchItem,
          proxyHolder: Option[ProxyIp],
          contentType: Option[String]): UrlResponse =
    get(link.url, link.refer.getOrElse(""), proxyHolder, contentType)

  def get(url: String,
          refer: String = "",
          proxyHolder: Option[ProxyIp] = None,
          contentType: Option[String] = None): UrlResponse = {
    val client = proxyHolder match {
      case Some(proxyIp) =>
        if (proxyIp.expired()) {
          defaultClient
        } else synchronized {
          LOG.info(s"Use proxy ${proxyIp} to fetch url ${url}")
          new HttpClientAgent(zhinangConf,
            proxyIp.host,
            proxyIp.port,
            HttpClientAgent.PROXY_SOCKS,
            false)
        }
      case None =>
        //new HttpClientAgent(zhinangConf)
        defaultClient
    }

    //只有text/html类型的网页才会继续提取内容，填充到response对象中
    contentType match {
      case Some(t) =>
        client.execute(url, refer, t)
      case None =>
        client.execute(url, refer)
    }
  }

  /**
    * 保存图片链接到一个图片文件中
    */
  def saveImage(url: String,
                imgFile: File,
                refer: String = "",
                proxyHolder: Option[ProxyIp] = None,
                contentType: String = "image"): Future[Boolean] = Future.successful {
    val response = get(url, refer, proxyHolder, Option(contentType))
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
  def jdoc(link: FetchItem,
           proxyHolder: Option[ProxyIp] = None,
           contentType: String = "text/html"): Document = {
    val response = get(link, proxyHolder, Option(contentType))
    Jsoup.parse(new ByteArrayInputStream(response.getContent),
      response.getEncoding,
      link.url
    )
  }

  def jdoc(url: String): Document = {
    val response = get(url)
    Jsoup.parse(new ByteArrayInputStream(response.getContent),
      response.getEncoding, url)
  }

  def jdoc(response: UrlResponse): Document = {
    Jsoup.parse(new ByteArrayInputStream(response.getContent),
      response.getEncoding,
      response.getUrl.toString)
  }
}

/**
  * 支持cookie的HTTP处理
  */
class CookieHttp(domain: String, cookiePath: String) {

  import org.apache.http.client.config.CookieSpecs
  import org.apache.http.client.methods.HttpGet
  import org.apache.http.client.protocol.HttpClientContext
  import org.apache.http.config.RegistryBuilder
  import org.apache.http.cookie.CookieSpecProvider
  import org.apache.http.impl.client.{BasicCookieStore, HttpClients}
  import org.apache.http.impl.cookie.{BasicClientCookie, BestMatchSpecFactory, BrowserCompatSpecFactory}
  import org.apache.http.util.EntityUtils


  private def getSessionId(httpResponse: CloseableHttpResponse): Option[String] = {
    // JSESSIONID
    if (httpResponse.getFirstHeader("Set-Cookie") != null) {
      val setCookie = httpResponse.getFirstHeader("Set-Cookie").getValue
      val sessionId = setCookie.substring("JSESSIONID=".length, setCookie.indexOf(";"))
      //println("JSESSIONID:" + sessionId)

      Option(sessionId)
    } else None
  }

  def get(url: String, sessionId: Option[String] = None): Response = {
    val httpGet = new HttpGet(url)

    val httpResponse = sessionId match {
      case Some(jsessionId) =>
        //根究sessionId构造cookie
        val cookieStore = new BasicCookieStore()

        // 新建一个Cookie
        val cookie = new BasicClientCookie("JSESSIONID", jsessionId)
        cookie.setVersion(0)
        cookie.setDomain(domain)
        cookie.setPath(cookiePath)
        cookieStore.addCookie(cookie)

        //设置上下文
        val context: HttpClientContext = HttpClientContext.create
        val registry = RegistryBuilder.create[CookieSpecProvider].register(CookieSpecs.BEST_MATCH, new BestMatchSpecFactory).register(CookieSpecs.BROWSER_COMPATIBILITY, new BrowserCompatSpecFactory).build
        context.setCookieSpecRegistry(registry)
        context.setCookieStore(cookieStore)

        val client = HttpClients.custom().setDefaultCookieStore(cookieStore).build();

        client.execute(httpGet)
      case None =>
        val client = HttpClients.createDefault

        client.execute(httpGet)
    }

    parseResponse(httpResponse)
  }

  private def parseResponse(httpResponse: CloseableHttpResponse): Response = {
    // 获取响应消息实体
    val entity = httpResponse.getEntity

    val code = httpResponse.getStatusLine.getStatusCode
    val sessionId = getSessionId(httpResponse)

    //    // 响应状态
    System.out.println("status:" + httpResponse.getStatusLine)
    System.out.println("headers:")
    val iterator = httpResponse.headerIterator
    while ( {
      iterator.hasNext
    }) System.out.println("\t" + iterator.next)

    // 判断响应实体是否为空
    val content = if (entity != null) {
      Option(EntityUtils.toByteArray(entity))
    } else {
      None
    }

    Response(code, sessionId, content)
  }

  case class Response(code: Int,
                      sessionId: Option[String],
                      content: Option[Array[Byte]])

}