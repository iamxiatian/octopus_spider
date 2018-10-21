package xiatian.octopus.storage.task

import java.io.File

import org.dizitart.no2.filters.Filters
import org.dizitart.no2.{Document, NitriteCollection, WriteResult}
import xiatian.octopus.common.MyConf
import xiatian.octopus.task.TopicTask

import scala.collection.JavaConverters._

object TaskDb extends NitriteDb(new File(MyConf.masterDbPath, "task.db")) {

  // 关键词引擎列表
  val topicCollection: NitriteCollection = db.getCollection("topic")


  def saveTopicTask(topic: TopicTask): WriteResult = {
    val doc = new Document()
    topic.toMap.foreach {
      case (k, v) => doc.put(k, v)
    }
    println(doc)
    val result = put(topicCollection, topic.id, doc)

    db.commit()

    result
  }

  def getTopicTasks(): List[TopicTask] =
    topicCollection.find().asScala.flatMap {
      doc =>
        TopicTask(doc)
    }.toList

  def getTopicTask(id: String): Option[TopicTask] =
    topicCollection.find(Filters.eq("id", id)).asScala.flatMap {
      doc => TopicTask(doc)
    }.headOption

  def close() = {
    db.close()
  }

  def main(args: Array[String]): Unit = {
    //TaskDb.saveEngine(SearchEngine("1", "百度新闻", "http://news.baidu.com/w={}", "utf-8"))
    //saveTopicTask(TopicTask("1", "", "默认主题", List("北京", "上海", "山东"), List("1")))


    println("_________________")
    println(getTopicTask("1"))

    db.close()
  }
}
