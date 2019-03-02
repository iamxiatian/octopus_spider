package xiatian.octopus.task

import java.io.{ByteArrayInputStream, DataInputStream}

import xiatian.octopus.actor.Context
import xiatian.octopus.common.Logging
import xiatian.octopus.model._
import xiatian.octopus.task.core.ArticleHubTask
import xiatian.octopus.task.epaper.EPaperTask


/**
  * 抓取任务，各类任务具有的基本行为. 典型的抓取任务如针对某个网站的全站采集，
  * 针对某个关键词的定向采集。
  */
abstract class FetchTask(id: String,
                         name: String,
                         taskType: TaskType
                        ) {
  /**
    * 是否接收该链接，作为本任务的一个抓取链接
    *
    * @param link
    */
  def accept(fetchItem: FetchItem): Boolean

  /**
    * 返回间隔多少秒之后会再次抓取的秒数, 如果永远不再抓取，返回None
    *
    * @param link
    * @return
    */
  def nextFetchSeconds(fetchItem: FetchItem): Option[Long] = None

  def entryItems: List[FetchItem] = ??? //该任务对应的入口链接

  /**
    * 把任务转换成二进制字节类型, 开始包含了两个整数，用于标记任务的类型和数据版本
    *
    * @return
    */
  def toBytes: Array[Byte]
}

object FetchTask extends Logging {

  def readFrom(bytes: Array[Byte]): Option[FetchTask] = {
    val din = new DataInputStream(new ByteArrayInputStream(bytes))

    val taskType = TaskType(din.readInt())

    val result = taskType match {
      case TaskType.ArticleHub =>
        ArticleHubTask(din)
      case TaskType.EPaper =>
        //电子报
        EPaperTask(din)
      case _ =>
        LOG.error(s"unknown task type $taskType")
        None
    }

    din.close()
    result
  }

  def context(taskId: String): Option[Context] = Option {
    //@TODO 修改此处
    //Context(FetchTask(""))
    null
  }

  def count(): Int = 0

  def get(link: FetchItem): Option[FetchTask] = get(link.taskId).orElse {
    LOG.warn(s"invalid task id ${link.taskId}, url=>${link.url}")
    None
  }

  def get(taskId: String): Option[FetchTask] = {

    val bytes = 0 //

    None
    //    taskId match {
    //      case TaskType
    //      case _ =>
    //        LOG.error(s"task $taskId does not exist.")
    //        None
    //    }
  }
}


