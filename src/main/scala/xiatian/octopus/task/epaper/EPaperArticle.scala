package xiatian.octopus.task.epaper

import xiatian.octopus.parse.ParsedData
import xiatian.octopus.storage.rdb.Repo

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Try

/**
  * 电子报的文章
  *
  * @param url
  * @param title
  * @param author
  * @param pubDate
  * @param column
  * @param rank
  * @param content
  */
case class EPaperArticle(id: String,
                         url: String,
                         title: String,
                         author: String,
                         pubDate: String, //yyyy-MM-dd格式的日期
                         media: String, //电子报来源, 如人民日报
                         page: String, //所在栏目
                         rank: Int, //在该栏目的排序序号
                         text: String,
                         html: String
                        ) extends ParsedData


object EPaperArticleDb extends Repo[EPaperArticle] {

  import profile.api._

  class ArticleTable(tag: Tag) extends
    Table[EPaperArticle](tag, "epaper_article") {

    def id = column[String]("id", O.PrimaryKey)

    def url = column[String]("url", O.Length(250))

    def title = column[String]("title", O.Length(250))

    def author = column[String]("author", O.Length(250))

    def pubDate = column[String]("pub_date", O.Length(50))

    def media = column[String]("media", O.Length(50))

    def page = column[String]("page", O.Length(50))

    def rank = column[Int]("rank")

    def text = column[String]("text", O.SqlType("MEDIUMTEXT"))

    def html = column[String]("html", O.SqlType("MEDIUMTEXT"))

    def * = (id, url, title, author, pubDate, media, page, rank, text, html) <>
      (EPaperArticle.tupled, EPaperArticle.unapply)
  }

  val entities = TableQuery[ArticleTable]

  def createSchema: Future[Try[Unit]] = db run {
    entities.schema.create.asTry
  }

  def dropSchema: Future[Try[Unit]] = db run {
    entities.schema.drop.asTry
  }

  def findById(id: String): Future[Option[EPaperArticle]] = db.run {
    entities.filter(_.id === id).result.headOption
  }

  def exists(id: String): Future[Boolean] = db run {
    entities.filter(_.id === id).exists.result
  }

  def count(): Future[Int] = db run {
    entities.length.result
  }

  def save(article: EPaperArticle) = exists(article.id) flatMap {
    case true =>
      LOG.info(s"${article.url} has existed, skip.")
      Future.successful(0)
    case false =>
      db run {
        entities += article
      }
  }

}
