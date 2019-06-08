package xiatian.octopus.expt

import xiatian.octopus.storage.rdb.Repo

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Try

case class Word(name: String,
                askCount: Int,
                replyCount: Int,
                totalCount: Int)

object WordRepo extends Repo[Word] {

  import profile.api._

  class WordTable(tag: Tag) extends
    Table[Word](tag, "word3") {

    def name = column[String]("name", O.Length(100), O.PrimaryKey)

    def askCount = column[Int]("ask_count")

    def replyCount = column[Int]("reply_count")

    def totalCount = column[Int]("total_count")

    def * = (name, askCount, replyCount, totalCount) <>
      (Word.tupled, Word.unapply)
  }

  val entities = TableQuery[WordTable]

  def createSchema: Future[Try[Unit]] = db run {
    entities.schema.create.asTry
  }

  def dropSchema: Future[Try[Unit]] = db run {
    entities.schema.drop.asTry
  }

  def list: Future[Seq[Word]] = db run {
    entities.result
  }

  def topAskWords(topN: Int): Seq[Word] = Await.result(
    db run {
      entities.sortBy(_.askCount.desc).take(topN).result
    }, Duration.Inf)


  def find(name: String): Future[Option[Word]] = db.run {
    entities.filter(_.name === name).result.headOption
  }

  def exists(name: String): Future[Boolean] = db run {
    entities.filter(_.name === name).exists.result
  }

  def count(): Future[Int] = db run {
    entities.length.result
  }

  def save(name: String, askCount: Int, replyCount: Int) = find(name) flatMap {
    case Some(_) =>
      db run {
        entities.filter(_.name === name)
          .map(r => (r.askCount, r.replyCount, r.totalCount))
          .update(askCount, replyCount, askCount + replyCount)
      }
    case None =>
      db run {
        entities += Word(name, askCount, replyCount, askCount + replyCount)
      }
  }

  def save(name: String, ask: Boolean) = find(name) flatMap {
    case Some(word) =>
      db run {
        val askCount = if (ask) word.askCount + 1 else word.askCount
        val replyCount = if (ask) word.replyCount else word.replyCount + 1
        val totalCount = askCount + replyCount

        entities.filter(_.name === name)
          .map(r => (r.askCount, r.replyCount, r.totalCount))
          .update(askCount, replyCount, totalCount)
      }
      Future.successful(0)
    case None =>
      db run {
        entities += Word(name, if (ask) 1 else 0, if (ask) 0 else 1, 1)
      }
  }

}

