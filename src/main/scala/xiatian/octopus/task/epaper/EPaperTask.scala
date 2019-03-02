package xiatian.octopus.task.epaper

import java.io.DataInputStream

import xiatian.octopus.model.FetchItem
import xiatian.octopus.task.{FetchTask, TaskType}

abstract class EPaperTask(id: String,
                          name: String)
  extends FetchTask(id, name, TaskType.EPaper) {
  /**
    * 是否接收该链接，作为本任务的一个抓取链接
    *
    * @param link
    */
  override def accept(fetchItem: FetchItem): Boolean = true

  def parseColumn(columItem: FetchItem): List[FetchItem]

  def parseArticle(articleItem: FetchItem): Article

  /**
    * 把任务转换成二进制字节类型, 开始包含了两个整数，用于标记任务的类型和数据版本
    *
    * @return
    */
  override def toBytes: Array[Byte] = ???
}

object EPaperTask {
  def apply(in: DataInputStream): Option[EPaperTask] = {
    val id = in.readUTF()
    id match {
      case 人民日报.id => Some(人民日报)
      case _ => None
    }

  }
}