package xiatian.octopus.actor

import org.joda.time.DateTime
import org.zhinang.protocol.http.UrlResponse
import xiatian.octopus.FastSerializable
import xiatian.octopus.model.FetchLink
import xiatian.octopus.task.FetchTask

/**
  * 在各个Actor之间进行通信的对象
  *
  * @author Tian Xia
  *         Dec 03, 2016 23:16
  */

case class ProxyIp(host: String, port: Int, expiredTimeInMillis: Long) {
  def expired(): Boolean = System.currentTimeMillis() > expiredTimeInMillis

  def address: String = s"$host:$port"

  override def toString(): String = {
    val d = new DateTime(expiredTimeInMillis).toString("MM-dd HH:mm:ss")
    s"$host:$port(expired at $d)"
  }
}

/**
  * 爬虫通过该消息向主控节点Master获取抓取任务
  *
  * @param id 爬虫的ID，爬虫的管理者根据该ID决定如何分配URL
  */
final case class FetchRequest(id: Int) extends FastSerializable

trait FetchJob extends FastSerializable

/**
  * Master在检测到新连接的爬虫Fetcher后，会自动发回该消息，
  * 通知爬虫以后利用该消息进行通信
  */
final case class InitFetchJob(id: String) extends FetchJob

final case class EmptyFetchJob() extends FetchJob

/**
  * 爬虫任务的上下文信息，由Master传递到Client
  */
final case class Context(task: FetchTask) extends FastSerializable

final case class NormalFetchJob(link: FetchLink,
                                context: Context,
                                proxy: Option[ProxyIp] = None
                               ) extends FetchJob

/**
  * link和fetcherId是最为基本的信息，后续的参数是为了由服务器传递到爬虫客户端，
  * 而不需要爬虫客户端去直接访问Master端的数据库
  */
case class Fetch(link: FetchLink,
                 fetcherId: Int,
                 context: Context,
                 proxy: Option[ProxyIp] = None) extends FastSerializable

/**
  * 网页中抽取出来的链接对象
  *
  * @param url    链接的URL
  * @param anchor 链接的锚文本
  * @param params 链接的额外参数，例如，有时需要抽取链接周边的日期存入该对象之中
  */
case class AnchorLink(url: String, anchor: String, params: Map[String, String])

/**
  * 对一个网页的抽取结果对象, 子链接可以用两种形式表示，即已经转换为FetchLink对象的子链接，
  * 尚未转换为FetchLink、原始的AnchorLink形式。
  *
  * @param link             处理的网页链接
  * @param childFetchLinks  该网页中抽取出来的子链接，已转换为FetchLink形式
  * @param childAnchorLinks 该网页中抽取出来的原始AnchorLink
  * @param code             抓取结果的代码
  * @param fetcherId        负责抓取的爬虫代号
  * @param message          抓取产生的信息描述文本，例如错误的时候，表示错误的内容
  */
case class FetchResult(fetcherId: Int,
                       code: Int,
                       link: FetchLink,
                       childFetchLinks: List[FetchLink] = List.empty,
                       childAnchorLinks: List[AnchorLink] = List.empty,
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