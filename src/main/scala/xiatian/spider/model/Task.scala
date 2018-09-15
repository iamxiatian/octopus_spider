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
sealed trait Task {
  def id: String

  def name: String

  //  /**
  //    * Minimum seconds for Refresh Cycle for Navigate Page(LP),
  //    *
  //    * @return
  //    */
  //  def navCycle: Long
  //
  //  /**
  //    * Minimum seconds for Refresh Cycle for Data Page(DP),
  //    *
  //    * @return
  //    */
  //  def dataCycle: Long
}

/**
  * XML类型的Task
  *
  */
case class XmlTask(id: String,
                   name: String,
                   navCycle: Long,
                   dataCycle: Long,
                   maxDepth: Int, //最大采集深度，超过该深度的列表页不再采集，入口深度为1
                   entries: List[Entry] = List.empty,
                   listPageRules: List[NavPageRule] = List.empty,
                   dataPageRules: List[DataPageRule] = List.empty
                  ) extends Task


object AmazonBookTask extends Task {
  val id = "AmazonTask"
  val name = "亚马逊图书任务"
}

object BingBookTask extends Task {
  val id = "BingTask"
  val name = "必应图书任务"
}

object Task {
  private val LOG = LoggerFactory.getLogger(Task.getClass)

  def context(taskId: String): Option[Context] = Some(Context())

  def count(): Int = 0

  def get(taskId: String): Option[Task] = {
    taskId match {
      case AmazonBookTask.id => Some(AmazonBookTask)
      case BingBookTask.id => Some(BingBookTask)
      case _ =>
        LOG.error(s"task $taskId does not exist.")
        None
    }
  }
}


