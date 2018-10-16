package xiatian.octopus.actor.master.db

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8

import xiatian.octopus.common.MyConf
import xiatian.octopus.task.Board

object BoardDb extends
  CachableListDb(
    //"/home/xiatian/workspace/github/spider/target/universal/stage/db/board"
    new File(MyConf.masterDbPath, "board").getCanonicalPath
  ) {

  def getBoardIds(): Seq[String] = {
    keys.map(new String(_, UTF_8)).toSeq
  }

  def getBoards(): Seq[Board] =
    elements(0, count()).map {
      case (k, v) => Board.readFrom(v)
    }.toSeq


  def save(board: Board) = {
    put(board.code.getBytes(UTF_8), board.toBytes())
    this
  }

  def get(boardId: String): Option[Board] =
    get(boardId getBytes UTF_8).map(Board.readFrom)


  def delete(boardId: String) = remove(boardId.getBytes(UTF_8))

  def delete(boardId: Long) = remove(boardId.toString.getBytes(UTF_8))

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

  def main(args: Array[String]): Unit = {
    BoardDb.getBoardIds()
      .flatMap(BoardDb.get)
      .foreach {
        board =>
          println(s"${board.code} \t ${board.name} \t ${board.entryUrls}")
      }
  }
}
