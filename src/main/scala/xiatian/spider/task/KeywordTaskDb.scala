package xiatian.spider.task

/**
  * 关键词采集任务数据库
  */
object KeywordTaskDb {


}

case class Keyword(word: String)

/**
  * 关键词搜索的引擎，例如百度新闻、知乎、搜索等
  *
  * @param name 关键词引擎的名称，如百度
  * @param queryPattern 关键词搜索的格式，例如： http://news.baidu.com/ns?word={}, 花括号中的内容会替换为具体的关键词
  * @param encoding 关键词的URL Encoding的编码
  */
case class KeywordEngine(
                        name: String,
                        queryPattern: String,
                        encoding: String = "UTF-8"
                        )