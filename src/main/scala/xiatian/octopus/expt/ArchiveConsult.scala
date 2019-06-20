package xiatian.octopus.expt

import java.sql.Timestamp

import xiatian.octopus.parse.ParsedData
import xiatian.octopus.storage.rdb.Repo
import xiatian.octopus.util.HashUtil

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
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
                          replier: String,
                          replyContent: String,
                          url: String,
                          urlMd5: String,
                          viewCount: Int,
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

    def replier = column[String]("replier", O.Length(50))

    def replyContent = column[String]("reply_content", O.SqlType("TEXT"))

    def url = column[String]("url", O.Length(250))

    def urlMd5 = column[String]("url_md5", O.Length(50), O.PrimaryKey)

    def viewCount = column[Int]("view_count")

    def category = column[String]("category", O.Length(20))

    def site = column[String]("site", O.Length(20))

    def * = (code, title, person, askTime, askContent, status, replyTime,
      replier, replyContent, url, urlMd5, viewCount, category, site) <>
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

  def listWithoutSite(site: String): Future[Seq[ArchiveConsult]] = db run {
    entities.filter(_.site =!= site).result
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

  def exists(code: String, site: String): Future[Boolean] = db run {
    entities.filter(e => e.code === code && e.site === site).exists.result
  }

  /**
    * 将字符串中的手机、电话号码、电子邮箱和身份证号码替换成名字形式，
    * 分别为：[PHONE], [TEL], [EMAIL]，[ID]
    * @param s
    * @return
    */
  def replace(s: String): String = {
    
  }

  /**
    * 把数据库里面的档案交互数据转存为XML格式，保存到filename对应的文件中
    */
  def toXML(filename: String) = {
    import scala.xml.XML

    val records = Await.result(ArchiveConsultDb.list, Duration.Inf)
    val items = records.map {
      record =>
        <record>
          <site>
            {record.site}
          </site>
          <url>
            {record.url}
          </url>
          <url_md5>
            {record.urlMd5}
          </url_md5>
          <code>
            {record.code}
          </code>
          <title>
            {record.title}
          </title>
          <person>
            {record.person}
          </person>
          <ask_time>
            {record.askTime}
          </ask_time>
          <ask_content>
            {record.askContent}
          </ask_content>
          <replier>
            {record.replier}
          </replier>
          <reply_time>
            {record.replyTime}
          </reply_time>
          <reply_content>
            {record.replyContent}
          </reply_content>
          <view_count>
            {record.viewCount}
          </view_count>
          <category>
            {record.category}
          </category>
        </record>
    }

    val doc = <records>
      {items}
    </records>

    XML.save(filename, doc, "UTF-8", true, null)
  }

  def main(args: Array[String]): Unit = {
    val filename = "/home/xiatian/writing/archive-consulting/data/dataset.xml"
    toXML(filename)
  }
}
