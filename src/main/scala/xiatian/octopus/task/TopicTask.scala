package xiatian.octopus.task

import xiatian.octopus.model.FetchLink

/**
  * 用户监控的主题，每个主题由多个关键词组成
  *
  * @param id             主题唯一ID
  * @param userId         主题隶属的用户ID
  * @param name           主题的显示名称
  * @param keywords       该主题下监控的关键词列表
  * @param maxDepth       最大采集深度
  * @param secondInterval 更新时间间隔
  */
case class TopicTask(id: String,
                     userId: String,
                     name: String,
                     keywords: List[String],
                     maxDepth: Int,
                     secondInterval: Long
                    ) extends ArticleHubTask {
  def toMap = Map(
    "id" -> id,
    "userId" -> userId,
    "name" -> name,
    "keywords" -> keywords.mkString(", "),
    "maxDepth" -> maxDepth,
    "secondInterval" -> secondInterval
  )

  /**
    * 该任务对应的入口链接
    *
    * @return
    */
  override def entryLinks: List[FetchLink] = keywords flatMap {
    keyword =>
      TopicEngine.engines.map(_._2).map(_.parseQuery(keyword))
  }


  /** 唯一的任务类型 */
  override def `type`: Int = FetchTask.TASK_TYPE_SITE

  /**
    * 根据url和锚文本，以及所在的页面链接，转换为FetchLink对象
    *
    * @param link
    * @param urlAnchorPairs
    * @return
    */
  override def makeChildLinks(link: FetchLink, urlAnchorPairs: Map[String, String]): List[FetchLink] = ???
}
