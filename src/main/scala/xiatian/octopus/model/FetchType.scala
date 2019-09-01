package xiatian.octopus.model

/**
  * 定义了各种抓取类型，链接的解析抽取、抽取结果保存都由抓取类型确定，
  * 如果采用trait，会导致akka远程通信出现问题
  */
abstract class FetchType extends Serializable {
  def id: Int

  def name: String

  def priority: Int
}

abstract class BaseFetchType(val id: Int,
                             val name: String,
                             val priority: Int = FetchType.PRIORITY_NORMAL)
  extends FetchType

object FetchType {
  final val PRIORITY_HIGH = 20
  final val PRIORITY_NORMAL = 10

  def get(id: Int): FetchType = id match {
    case ArticlePage.id => ArticlePage
    case HubPage.id => HubPage
    case EPaper.Column.id => EPaper.Column
    case EPaper.Article.id => EPaper.Article
    case _ => Unknown
  }

  def all: List[FetchType] = List(
    ArticlePage,
    HubPage,
    EPaper.Column,
    EPaper.Article
  )


  case object Unknown extends BaseFetchType(0, "未知抓取条目")

  /**
    * 文章类型的链接
    */
  case object ArticlePage extends BaseFetchType(1, "文章链接")

  /**
    * 列表导航类型的连接
    */
  case object HubPage extends BaseFetchType(2, "导航链接")

  object EPaper {

    case object Column extends BaseFetchType(51, "电子报栏目")

    case object Article extends BaseFetchType(52, "电子报文章")

  }
}

