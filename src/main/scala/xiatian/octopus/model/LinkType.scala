package xiatian.octopus.model

/**
  * 定义了各种链接类型，链接的解析抽取、抽取结果保存都由链接类型确定
  */
sealed trait LinkType {
  def id: Int

  def name: String

  def priority = LinkType.PRIORITY_NORMAL
}

case object UnknownLinkType extends LinkType {
  override val id: Int = 0

  override val name: String = "UnknownLink"
}

/**
  * 文章类型的链接
  */
case object ArticleLinkType extends LinkType {
  override val id: Int = 1

  override val name: String = "ArticleLink"
}

/**
  * 列表导航类型的连接
  */
case object HubLinkType extends LinkType {
  override val id: Int = 2

  override val name: String = "HubPage"
}

object LinkType {
  final val PRIORITY_HIGH = 20
  final val PRIORITY_NORMAL = 10

  def apply(id: Int): LinkType = id match {
    case ArticleLinkType.id => ArticleLinkType
    case HubLinkType.id => HubLinkType
    case _ => UnknownLinkType
  }

  def all: List[LinkType] = List(
    ArticleLinkType,
    HubLinkType
  )
}

