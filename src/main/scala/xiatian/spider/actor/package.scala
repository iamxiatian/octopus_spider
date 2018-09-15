package xiatian.spider

import java.net.URL

import xiatian.spider.model.FetchLink

/**
  * 定义package级别的隐式转换对象，以精简代码的编写
  *
  * @author Tian Xia
  *         Dec 06, 2016 11:17
  */
package object actor {
  implicit def fetchLink2Url(fetchLink: FetchLink): String = fetchLink.url

  implicit def string2url(s: String): URL = new URL(s)
}
