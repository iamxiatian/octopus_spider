package xiatian.octopus.model

/**
  * 定义了各种链接类型，链接的解析抽取、抽取结果保存都由链接类型确定
  */
abstract class FetchType(id: Int, name: String, priority: Int = FetchType.PRIORITY_NORMAL) {
  //  def id: Int = ???
  //
  //  def name: String = ???

  //  def priority = FetchType.PRIORITY_NORMAL
}

object FetchType {
  final val PRIORITY_HIGH = 20
  final val PRIORITY_NORMAL = 10

  def apply(id: Int): FetchType = id match {
    case ArticlePage.id => ArticlePage
    case HubPage.id => HubPage
    case _ => Unknown
  }

  def all: List[FetchType] = List(
    ArticlePage,
    HubPage
  )


  case object Unknown extends FetchType(0, "未知抓取条目")

  /**
    * 文章类型的链接
    */
  case object ArticlePage extends FetchType(1, "文章链接")

  /**
    * 列表导航类型的连接
    */
  case object HubPage extends FetchType(2, "导航链接")

  object EPaper {

    case object Column extends FetchType(51, "电子报栏目链接")

  }


}

