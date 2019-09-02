package xiatian.octopus.task

/**
  * 任务的类型
  */
sealed trait TaskCategory {
  def id: Int

  def name: String
}

abstract class BaseTaskCategory(val id: Int, val name: String) extends TaskCategory

object TaskCategory {

  case object Unknown extends BaseTaskCategory(0, "未知任务类型")

  case object ArticleHub extends BaseTaskCategory(1, "文章-导航任务类型")

  case object EPaper extends BaseTaskCategory(51, "电子报任务")

  def apply(id: Int): TaskCategory = id match {
    case ArticleHub.id => ArticleHub
    case EPaper.id => EPaper
    case _ => Unknown
  }

  def main(args: Array[String]): Unit = {

    val x = 400000000
    val y = x * 30

    print(y)

  }
}