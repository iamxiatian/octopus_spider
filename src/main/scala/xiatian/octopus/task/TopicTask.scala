package xiatian.octopus.task

import org.dizitart.no2.Document
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

}

object TopicTask {
  def apply(doc: Document): Option[TopicTask] = if (doc == null) None else {
    val id = doc.get("id").asInstanceOf[String]
    val userId = doc.get("userId").asInstanceOf[String]
    val name = doc.get("name").asInstanceOf[String]
    val keywords = doc.get("keywords").asInstanceOf[String].split(",").map(_.trim).toList
    val maxDepth = doc.get("maxDepth").asInstanceOf[Int]
    val secondInterval = doc.get("secondInterval").asInstanceOf[Long]
    Some(TopicTask(id, userId, name, keywords, maxDepth, secondInterval))
  }
}
