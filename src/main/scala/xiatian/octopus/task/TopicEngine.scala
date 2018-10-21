package xiatian.octopus.task

import org.slf4j.LoggerFactory

/**
  * 主题搜索的引擎，例如百度新闻、知乎、搜索等
  *
  * @param id           搜索引擎的id，唯一不变
  * @param name         搜索引擎的名称，如百度
  * @param queryPattern 搜索的格式，例如： http://news.baidu.com/ns?word={}, 花括号中的内容会替换为具体的关键词
  * @param encoding     输入关键词的URL Encoding的编码
  */
case class TopicEngine(
                        id: String,
                        name: String,
                        queryPattern: String,
                        encoding: String = "UTF-8"
                      ) {
  def toMap = Map(
    "id" -> id,
    "name" -> name,
    "queryPattern" -> queryPattern,
    "encoding" -> encoding
  )
}

object TopicEngine {
  private val LOG = LoggerFactory.getLogger(TopicEngine.getClass)

  /**
    * 从conf/topic-engine.xml中加载主题引擎
    * @return
    */
  private def load(): Map[String, TopicEngine] = {
    import scala.xml.XML

    val doc = XML.loadFile("./conf/topic-engine.xml")
    (doc \\ "engine").map {
      e =>
        val id = (e \ "@id").text.trim
        val name = (e \ "@name").text.trim
        val pattern = (e \ "query-pattern").text.trim
        val encoding = (e \ "query-pattern" \ "@encoding").text.trim
        TopicEngine(id, name, pattern, if (encoding.isEmpty) "utf-8" else encoding)
    }.filter {
      engine =>
        if (engine.id.isEmpty || engine.queryPattern.isEmpty) {
          LOG.warn(s"skip empty topic engine ==> $engine")
          false
        } else true
    }.map {
      engine => (engine.id, engine)
    }.toMap
  }

  lazy val engines = load()


}