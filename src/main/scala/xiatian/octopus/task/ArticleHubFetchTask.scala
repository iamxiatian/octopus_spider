package xiatian.octopus.task

import xiatian.octopus.model._

/**
  * 文章和列表构成的抓取任务
  */
case class ArticleHubFetchTask(
                                id: String,
                                name: String,
                                maxDepth: Int,
                                refreshInterval: Long
                              ) extends FetchTask {

  /** 该任务下可以处理的链接类型 */
  override def types: Set[LinkType] = Set(ArticleLink, HubLink)

  /**
    * 是否接收该链接，作为本任务的一个抓取链接. 如果可以，返回link，否则返回None
    *
    * @param link
    */
  override def filter(link: FetchLink): Option[FetchLink] = link.`type` match {
    case ArticleLink =>
      if (link.depth > maxDepth + 1) None
      else Some(link)
    case HubLink =>
      if (link.depth > maxDepth) None else Some(link)
    case _ => None
  }

  /**
    * 返回间隔多少秒之后会再次抓取的秒数, 如果永远不再抓取，返回None
    *
    * @param link
    * @return
    */
  override def nextFetchSeconds(link: FetchLink): Option[Long] = link.`type` match {
    case ArticleLink => None //文章链接永不重复抓取
    case HubLink =>
      val depth = if (link.depth > 1) link.depth else 1
      val seconds = refreshInterval * (Math.pow(2, link.retries)).toLong * depth
      Some(seconds)
  }
}
