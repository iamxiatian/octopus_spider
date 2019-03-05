package xiatian.octopus.parse

import org.zhinang.protocol.http.UrlResponse
import xiatian.octopus.model.FetchItem
import xiatian.octopus.task.epaper.人民日报
import xiatian.octopus.task.{FetchTask, TaskType}

import scala.util.Try

/**
  * Parser负责解析一个抓取条目，获取其中的结果
  */
trait Parser {
  def parse(item: FetchItem, response: UrlResponse): Try[ParseResult]

}

object Parser {
  def get(taskId: String): Option[Parser] =
    FetchTask.get(taskId).flatMap(t => get(t))

  def get(task: FetchTask): Option[Parser] = {
    task.taskType match {
      case TaskType.EPaper =>
        if (task.id == 人民日报.id)
          Option(人民日报)
        else None
      case _ => None
    }
  }
}
