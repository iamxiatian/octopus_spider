package xiatian.spider.model

import org.slf4j.LoggerFactory
import xiatian.spider.actor.Context


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
  * 任务特质，各类任务具有的基本行为
  */
sealed trait FetchTask {
  def id: String

  def name: String
}

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
                  ) extends FetchTask


object FetchTask {
  private val LOG = LoggerFactory.getLogger(FetchTask.getClass)

  def context(taskId: String): Option[Context] = Some(Context())

  def count(): Int = 0

  def get(taskId: String): Option[FetchTask] = {
    taskId match {
      //case AmazonBookTask.id => Some(AmazonBookTask)
      case _ =>
        LOG.error(s"task $taskId does not exist.")
        None
    }
  }
}


