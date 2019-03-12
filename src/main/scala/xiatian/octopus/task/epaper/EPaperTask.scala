package xiatian.octopus.task.epaper

import java.io.{DataInputStream, DataOutputStream}

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

  override def writeBody(dos: DataOutputStream): Unit = {
    dos.writeUTF(id)
    dos.writeUTF(name)
  }
}

object EPaperTask {
  def apply(in: DataInputStream): Option[EPaperTask] = {
    val id = in.readUTF()
    id match {
      case 人民日报.id => Some(人民日报)
      case 光明日报.id => Some(光明日报)
      case _ => None
    }

  }
}