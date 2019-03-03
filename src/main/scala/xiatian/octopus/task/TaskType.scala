package xiatian.octopus.task

sealed trait TaskType {
  def id: Int

  def name: String
}

abstract class BaseTaskType(val id: Int, val name: String)
  extends TaskType

object TaskType {

  case object Unknown extends BaseTaskType(0, "未知任务类型")

  case object ArticleHub extends BaseTaskType(1, "文章-导航任务类型")

  case object EPaper extends BaseTaskType(51, "电子报任务")

  def apply(id: Int): TaskType = id match {
    case ArticleHub.id => ArticleHub
    case EPaper.id => EPaper
    case _ => Unknown
  }
}