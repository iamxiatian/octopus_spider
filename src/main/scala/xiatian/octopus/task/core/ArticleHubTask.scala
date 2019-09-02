package xiatian.octopus.task.core

import java.io.{DataInputStream, DataOutputStream}

import xiatian.octopus.model.{FetchItem, FetchType}
import xiatian.octopus.task.{FetchTask, TaskCategory}

/**
  * 由文章网页Article和中心网页Hub构成的抓取任务
  */
class ArticleHubTask(id: String,
                     name: String,
                     entryItems: List[FetchItem],
                     maxDepth: Int,
                     secondInterval: Long
                    )
  extends FetchTask(id, name, TaskCategory.ArticleHub) {
  /**
    * 是否接收该链接，作为本任务的一个抓取链接. 如果可以，返回link，否则返回None
    *
    * @param link
    */
  override def accept(item: FetchItem): Boolean =
    item.`type` match {
      case FetchType.ArticlePage =>
        item.depth <= maxDepth + 1
      case _ =>
        item.depth <= maxDepth
    }

  /**
    * 返回间隔多少秒之后会再次抓取的秒数, 如果永远不再抓取，返回None
    *
    * @param link
    * @return
    */
  override def nextFetchSeconds(item: FetchItem): Option[Long] =
    item.`type` match {
      case FetchType.ArticlePage => None //文章链接永不重复抓取
      case _ =>
        val seconds = secondInterval * Math.pow(2, item.retries) * Math.pow(2, item.depth - 1)
        Some(seconds.toLong)
    }

  /**
    * 把任务转换成二进制字节类型, 开始包含了两个整数，用于标记任务的类型和数据版本
    *
    * @return
    */
  override def writeBody(dos: DataOutputStream) = {
    super.writeBody(dos)
    dos.writeInt(maxDepth)
    dos.writeLong(secondInterval)
  }
}

object ArticleHubTask {
  def apply(in: DataInputStream): Option[ArticleHubTask] = {
    val id = in.readUTF()
    val name = in.readUTF()

    val entryCount = in.readInt()
    val entryItems = (1 to entryCount).toList.map {
      _ =>
        FetchItem.readFrom(in)
    }

    val maxDepth = in.readInt()
    val secondInterval = in.readLong()

    Some(new ArticleHubTask(id, name, entryItems, maxDepth, secondInterval))
  }
}