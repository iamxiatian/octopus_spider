package xiatian.octopus.model

import org.slf4j.LoggerFactory
import xiatian.octopus.actor.Context


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
  def types: Set[LinkType]

  /**
    * 是否接收该链接，作为本任务的一个抓取链接. 如果可以，返回link，否则返回None
    *
    * @param link
    */
  def filter(link: FetchLink): Option[FetchLink]

  /**
    * 返回间隔多少秒之后会再次抓取的秒数, 如果永远不再抓取，返回None
    *
    * @param link
    * @return
    */
  def nextFetchSeconds(link: FetchLink): Option[Long]
}


/**
  *
  * @author Tian Xia
  *         Nov 27, 2016 17:08
  */

case class Entry(url: String,
                 params: Map[String, String] = Map.empty[String, String])


case class NavPageRule()

case class DataPageRule()

/**
  * XML类型的Task
  */
case class XmlTask(id: String,
                   name: String,
                   navCycle: Long,
                   dataCycle: Long,
                   maxDepth: Int, //最大采集深度，超过该深度的列表页不再采集，入口深度为1
                   entries: List[Entry] = List.empty,
                   listPageRules: List[NavPageRule] = List.empty,
                   dataPageRules: List[DataPageRule] = List.empty
                  )


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


