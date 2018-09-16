package xiatian.spider.task

import org.slf4j.LoggerFactory
import xiatian.spider.model._

/**
  * 基于XML任务描述文件，构造Board，形成抓取任务，和常用的ArticleHubFetchTask功能
  * 基本类似，只是该类提供了更多的链接过滤条件。
  *
  * @param board
  */
case class BoardFetchTask(board: Board) extends FetchTask {
  private val LOG = LoggerFactory.getLogger(BoardFetchTask.getClass.getName)

  /** 任务 ID */
  override def id: String = board.code

  /** 任务名称 */
  override def name: String = board.name

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
      else {
        if (board.articleFilter.urlRule.r.pattern.matcher(link.url).matches()
          && !board.articleFilter.notUrlRule.r.pattern.matcher(link.url).matches()
          && board.articleFilter.minAnchorLength <= link.anchor.length) {
          //文章链接
          if (LOG.isDebugEnabled) {
            LOG.debug(s"Article link: $anchor ==> $url")
          }

          //如果是可跳转的链接，获取其跳转后的链接
          val transformed = UrlTransformer.transform(url)
          val articleUrl = if (transformed.isEmpty) url else transformed.get

          if (articleUrl.isEmpty) {
            log.warn(s"can not extract followed url from $url")
            None
          } else {
            if (articleUrl != url) {
              log.info(s"extracted article $articleUrl from $url")
            }
            Some(FetchLink(
              articleUrl,
              Some(link.url),
              anchor,
              link.depth + 1,
              FetchLink.ARTICLE_DATA_TYPE,
              link.boardId,
              link.params + ("loc" -> ((idx + 1) * 100 / totalPairs).toString)
            )) //把链接在页面中的位置，转换为0，100之间的数字
          }
        } else if (context.nextPageUrlRule.r.pattern.matcher(url).matches()) {
          Some(FetchLink(url, Some(link.url), anchor, link.depth + 1, FetchLink.ARTICLE_NAV_TYPE,
            link.boardId, link.params))
        } else {
          log.debug(s"SKIP link: $anchor ==> $url")

          None
        }
      }

        Some(link)
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
