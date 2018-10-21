package xiatian.octopus.task

import org.dizitart.no2.Document

/**
  * 用户监控的主题，每个主题由多个关键词组成
  *
  * @param id        主题唯一ID
  * @param userId    主题隶属的用户ID
  * @param name      主题的显示名称
  * @param keywords  该主题下监控的关键词列表
  * @param engineIds 该主题监控的来源引擎的ID列表
  */
case class TopicTask(id: String,
                     userId: String,
                     name: String,
                     keywords: List[String],
                     engineIds: List[String]
                    ) {
  def toMap = Map(
    "id" -> id,
    "userId" -> userId,
    "name" -> name,
    "keywords" -> keywords.mkString(", "),
    "engineIds" -> engineIds.mkString(", ")
  )
}

object TopicTask {
  def apply(doc: Document): Option[TopicTask] = if (doc == null) None else {
    val id = doc.get("id").asInstanceOf[String]
    val userId = doc.get("userId").asInstanceOf[String]
    val name = doc.get("name").asInstanceOf[String]
    val keywords = doc.get("keywords").asInstanceOf[String].split(",").map(_.trim).toList
    val engineIds = doc.get("engineIds").asInstanceOf[String].split(",").map(_.trim).toList
    Some(TopicTask(id, userId, name, keywords, engineIds))
  }
}
