package xiatian.octopus.expt

import java.sql.Timestamp

import xiatian.octopus.parse.ParsedData
import xiatian.octopus.storage.rdb.Repo
import xiatian.octopus.util.HashUtil

import scala.concurrent.Future
import scala.util.Try

/**
  */
case class ArchiveConsult(code: String,
                          title: String,
                          person: String,
                          askTime: Timestamp,
                          askContent: String,
                          status: String,
                          replyTime: Timestamp,
                          replyContent: String,
                          url: String,
                          urlMd5: String,
                          category: String,
                          site: String
                         ) extends ParsedData


object ArchiveConsultDb extends Repo[ArchiveConsult] {

  import profile.api._

  class ArchiveConsultTable(tag: Tag) extends
    Table[ArchiveConsult](tag, "archive") {

    def code = column[String]("code", O.Length(50))


    def title = column[String]("title", O.Length(250))

    def person = column[String]("person", O.Length(50))

    def askTime = column[Timestamp]("ask_time", O.SqlType("datetime"))

    def askContent = column[String]("ask_content", O.SqlType("TEXT"))

    def status = column[String]("status", O.Length(20))

    def replyTime = column[Timestamp]("reply_time", O.SqlType("datetime"))

    def replyContent = column[String]("reply_content", O.SqlType("TEXT"))

    def url = column[String]("url", O.Length(250))

    def urlMd5 = column[String]("url_md5", O.Length(50), O.PrimaryKey)

    def category = column[String]("category", O.Length(20))

    def site = column[String]("site", O.Length(20))

    def * = (code, title, person, askTime, askContent, status, replyTime,
      replyContent, url, urlMd5, category, site) <>
      (ArchiveConsult.tupled, ArchiveConsult.unapply)
  }

  val entities = TableQuery[ArchiveConsultTable]

  def createSchema: Future[Try[Unit]] = db run {
    entities.schema.create.asTry
  }

  def dropSchema: Future[Try[Unit]] = db run {
    entities.schema.drop.asTry
  }

  def list: Future[Seq[ArchiveConsult]] = db run {
    entities.result
  }

  def findByCode(code: String): Future[Option[ArchiveConsult]] = db.run {
    entities.filter(_.code === code).result.headOption
  }

  def existByUrl(url: String): Future[Boolean] = db run {
    val md5 = HashUtil.md5(url)
    entities.filter(_.urlMd5 === md5).exists.result
  }

  def count(): Future[Int] = db run {
    entities.length.result
  }

  def save(article: ArchiveConsult) = existByUrl(article.url) flatMap {
    case true =>
      println(s"${article.url} has existed, skip.")
      Future.successful(0)
    case false =>
      db run {
        entities += article
      }
  }

}
