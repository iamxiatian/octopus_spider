package xiatian.spider.model

import xiatian.spider.parse.{EmptyExtractor, EmptySaver, Extractor, Saver}

/**
  * 定义了各种链接类型，链接的解析抽取、抽取结果保存都由链接类型确定
  */
sealed trait LinkType {
  final val PRIORITY_HIGH = 20
  final val PRIORITY_NORMAL = 10

  def id: Int

  def name: String

  def priority = PRIORITY_NORMAL

  //该类型链接的默认更新频度为1000年，即不再更新
  def refreshInSeconds = 86400L * 365 * 1000

  /**
    * 该类型链接对应的抽取器
    */
  def extractor: Extractor = EmptyExtractor

  /**
    * 该类型链接对应抽取结果的保存器, 默认为空保存器，即扔掉抽取的内容
    */
  def saver: Saver = EmptySaver
}

case object UnknownLink extends LinkType {
  override val id: Int = 0

  def name: String = "UnknownLink"
}

/**
  * 文章类型的链接
  */
case object ArticleLink extends LinkType {
  override val id: Int = 1

  def name: String = "ArticleLink"

  override def extractor: Extractor = EmptyExtractor

  override def saver: Saver = EmptySaver
}

/**
  * 列表导航类型的连接
  */
case object HubLink extends LinkType {
  override val id: Int = 2

  def name: String = "HubPage"

  override def extractor: Extractor = EmptyExtractor

  override def saver: Saver = EmptySaver

  //更新频率为7天
  override def refreshInSeconds = 86400L * 7

}

object LinkType {
  def apply(id: Int): LinkType = id match {
    case ArticleLink.id => ArticleLink
    case HubLink.id => HubLink
    case _ => UnknownLink
  }

  def all: List[LinkType] = List(
    ArticleLink,
    HubLink
  )
}

