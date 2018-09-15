package xiatian.spider.actor

import org.zhinang.protocol.http.UrlResponse
import xiatian.spider.FastSerializable
import xiatian.spider.model.FetchLink

/**
  * 在各个Actor之间进行通信的对象
  *
  * @author Tian Xia
  *         Dec 03, 2016 23:16
  */

case class ProxyIp(host: String, port: Int, expiredTimeInMillis: Long) {
  def expired(): Boolean = System.currentTimeMillis() > expiredTimeInMillis
}

/**
  * 爬虫通过该消息向主控节点Master获取抓取任务
  *
  * @param id 爬虫的ID，爬虫的管理者根据该ID决定如何分配URL
  */
final case class FetchRequest(id: Int) extends FastSerializable

trait FetchTask extends FastSerializable

/**
  * Master在检测到新连接的爬虫Fetcher后，会自动发回该消息，
  * 通知爬虫以后利用该消息进行通信
  */
final case class InitFetchTask(id: String) extends FetchTask

final case class EmptyFetchTask() extends FetchTask

/**
  * 爬虫任务的上下文信息，由Master传递到Client
  */
final case class Context() extends FastSerializable

final case class NormalFetchTask(link: FetchLink,
                                 context: Context,
                                 proxy: Option[ProxyIp] = None
                                ) extends FetchTask

/**
  * link和fetcherId是最为基本的信息，后续的参数是为了由服务器传递到爬虫客户端，
  * 而不需要爬虫客户端去直接访问Master端的数据库
  */
case class Fetch(link: FetchLink,
                 fetcherId: Int,
                 context: Context,
                 proxy: Option[ProxyIp] = None) extends FastSerializable

case class FetchFinished(link: FetchLink,
                         childLinks: List[FetchLink],
                         code: Int,
                         fetcherId: Int,
                         message: Option[String] = None) extends FastSerializable

case class FetchFailure(link: FetchLink,
                        reason: String,
                        fetcherId: Int) extends FastSerializable

case class FetchContent(link: FetchLink,
                        context: Context,
                        source: String,
                        title: String,
                        author: String,
                        findPubDate: Boolean, //是否抽取出发布时间
                        pubDate: java.util.Date,
                        sentiment: Float,
                        keywords: List[String],
                        plainContent: String,
                        formattedContent: String,
                        description: String) extends FastSerializable

//请求获取爬虫运行状态
case class FetchStatsRequest() extends FastSerializable

case class FetchStatsReply(msg: String) extends FastSerializable

object FetchCode {
  val Ok = 200

  val Not_HTML = 901
  val UnknownHost = UrlResponse.Code_UnknownHost
  val Error = 500
  val PARSE_ERROR = 501

  def isOk(code: Int): Boolean = {
    code == Ok
  }
}