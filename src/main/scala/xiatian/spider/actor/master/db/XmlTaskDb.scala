package xiatian.spider.actor.master.db

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8

import xiatian.common.MyConf

object XmlTaskDb extends
  CachableListDb(
    new File(MyConf.masterDbPath, "task").getCanonicalPath
  ) {

  def getBoardIds(): Seq[String] = {
    keys.map(new String(_, UTF_8)).toSeq
  }

  //  def getBoards(): Seq[XmlTask] =
  //    elements(0, count()).map {
  //      case (k, v) => XmlTask.readFrom(v)
  //    }.toSeq
  //
  //
  //  def save(task: XmlTask) = {
  //    put(task.id.getBytes(UTF_8), task.toBytes())
  //    this
  //  }
  //
  //  def get(boardId: String): Option[Board] =
  //    get(boardId getBytes UTF_8).map(Board.readFrom)


  def delete(taskId: String) = remove(taskId.getBytes(UTF_8))

  /**
    * 显示从下标从start开始，到end截至（包括）的频道ID
    */
  def getBoardIds(startInclude: Int, endInclude: Int): Seq[String] =
    keys.drop(startInclude - 1)
      .take(endInclude - startInclude + 1)
      .map(new String(_, UTF_8))
      .toSeq

  /**
    * 该频道是否存在
    */
  def exists(boardId: String): Boolean = keys.contains(boardId.getBytes(UTF_8))

  def exists(boardId: Long): Boolean = exists(boardId.toString)

}
