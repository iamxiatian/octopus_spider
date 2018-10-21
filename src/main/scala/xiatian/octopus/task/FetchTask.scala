package xiatian.octopus.task

import org.slf4j.LoggerFactory
import xiatian.octopus.actor.Context
import xiatian.octopus.model._


/**
  * 任务特质，各类任务具有的基本行为. 典型的抓取任务如针对某个网站的全站采集，
  * 针对某个关键词的定向采集。
  */
trait FetchTask {
  /** 任务 ID */
  def id: String

  /** 任务名称 */
  def name: String

  /** 该任务下可以处理的链接类型 */
  def types: Set[LinkType] = Set.empty

  /**
    * 是否接收该链接，作为本任务的一个抓取链接. 如果可以，返回link，否则返回None
    *
    * @param link
    */
  def filter(link: FetchLink): Option[FetchLink] = None

  /**
    * 返回间隔多少秒之后会再次抓取的秒数, 如果永远不再抓取，返回None
    *
    * @param link
    * @return
    */
  def nextFetchSeconds(link: FetchLink): Option[Long] = None

  /**
    * 该任务对应的入口链接
    *
    * @return
    */
  def entryLinks: List[FetchLink]
}

/**
  * 由文章网页Article和中心网页Hub构成的抓取任务
  */
private[task] trait ArticleHubTask extends FetchTask {
  def maxDepth: Int

  def secondInterval: Long

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
      val seconds = secondInterval * Math.pow(2, link.retries) * Math.pow(2, link.depth - 1)
      Some(seconds.toLong)
  }
}


object FetchTask {
  private val LOG = LoggerFactory.getLogger(FetchTask.getClass)

  def context(taskId: String): Option[Context] = Some(Context())

  def count(): Int = 0

  def get(link: FetchLink): Option[FetchTask] = get(link.taskId).orElse {
    LOG.warn(s"invalid task id ${link.taskId}, url=>${link.url}")
    None
  }

  def get(taskId: String): Option[FetchTask] = {
    taskId match {
      //case AmazonBookTask.id => Some(AmazonBookTask)
      case _ =>
        LOG.error(s"task $taskId does not exist.")
        None
    }
  }
}


