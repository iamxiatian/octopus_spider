package xiatian.octopus.task

abstract class TaskType(id: Int, name: String)

object TaskType {

  case object Unknown extends TaskType(0, "未知任务类型")

  case object ArticleHub extends TaskType(1, "文章-导航任务类型")

  case object EPaper extends TaskType(51, "电子报任务")

  def apply(id: Int): TaskType = id match {
    case ArticleHub.id => ArticleHub
    case EPaper.id => EPaper
    case _ => Unknown
  }
}