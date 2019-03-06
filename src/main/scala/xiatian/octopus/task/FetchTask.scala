package xiatian.octopus.task

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import xiatian.octopus.common.{Logging, MyConf}
import xiatian.octopus.model._
import xiatian.octopus.parse.Parser
import xiatian.octopus.storage.master.TaskDb
import xiatian.octopus.task.core.ArticleHubTask
import xiatian.octopus.task.epaper.{EPaperTask, 人民日报}


/**
  * 抓取任务，各类任务具有的基本行为. 典型的抓取任务如针对某个网站的全站采集，
  * 针对某个关键词的定向采集。
  */
abstract class FetchTask(val id: String,
                         val name: String,
                         val taskType: TaskType
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
  def nextFetchSeconds(fetchItem: FetchItem): Option[Long] = Some(MyConf.MaxTimeSeconds)

  def entryItems: List[FetchItem] = ??? //该任务对应的入口链接

  /**
    * 把任务转换成二进制字节类型, 开始包含了两个整数，用于标记任务的类型和数据版本
    *
    * @return
    */
  def toBytes: Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val dos = new DataOutputStream(out)

    //写入头部信息
    dos.writeInt(taskType.id)

    //写入主体信息
    writeBody(dos)

    dos.close()
    out.close()

    out.toByteArray
  }

  /**
    * 把任务的描述主题，写入到输出流之中
    */
  protected def writeBody(dos: DataOutputStream): Unit = {
    dos.writeUTF(id)
    dos.writeUTF(name)
    dos.writeInt(entryItems.size)
    entryItems foreach {
      item =>
        dos.write(item.toBytes())
    }
  }

  /**
    * 该任务对应的解析器, 利用该解析器可以对抓取条目进行解析，获取其中的内容
    *
    * @return
    */
  def parser: Option[Parser] = None
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
        LOG.error(s"俺不懂该任务类型： $taskType")
        None
    }

    din.close()
    result
  }

  def count(): Int = TaskDb.count()

  def context(taskId: String): Context = Context()

  def get(link: FetchItem): Option[FetchTask] = get(link.taskId)

  def get(taskId: String): Option[FetchTask] = {
    TaskDb.get(taskId).flatMap(readFrom(_))
  }

  def main(args: Array[String]): Unit = {
    TaskDb.save(人民日报)
    TaskDb.getIds().foreach {
      id =>
        val task = get(id)
        println("_____________________")
        task.map {
          t =>
            t.entryItems.foreach(println)
        }
    }
    TaskDb.close()
  }
}


