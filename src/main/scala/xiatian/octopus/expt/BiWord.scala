package xiatian.octopus.expt

import xiatian.octopus.storage.rdb.Repo

import scala.concurrent.Future
import scala.util.Try


case class BiWord(bigram: String,
                  amount: Int)


object BiWordRepo extends Repo[BiWord] {

  import profile.api._

  class BiWordTable(tag: Tag) extends
    Table[BiWord](tag, "biword3") {

    def bigram = column[String]("bigram", O.Length(200), O.PrimaryKey)

    def amount = column[Int]("amount")

    def * = (bigram, amount) <>
      (BiWord.tupled, BiWord.unapply)
  }

  val entities = TableQuery[BiWordTable]

  def createSchema: Future[Try[Unit]] = db run {
    entities.schema.create.asTry
  }

  def dropSchema: Future[Try[Unit]] = db run {
    entities.schema.drop.asTry
  }

  def list: Future[Seq[BiWord]] = db run {
    entities.result
  }

  def list(topN: Int): Future[Seq[BiWord]] = db run {
    entities.sortBy(_.amount.desc).take(topN).result
  }


  def makeBigram(name1: String, name2: String) =
    if (name1 > name2) s"${name2}-${name1}" else s"${name1}-${name2}"


  def find(bigram: String): Future[Option[BiWord]] = db.run {
    entities.filter(_.bigram === bigram).result.headOption
  }

  def exists(bigram: String): Future[Boolean] = db run {
    entities.filter(_.bigram === bigram).exists.result
  }

  def count(): Future[Int] = db run {
    entities.length.result
  }

  def save(bigram: String, adder: Int) = {
    find(bigram) flatMap {
      case Some(word) =>
        db run {
          entities.filter(_.bigram === bigram)
            .map(_.amount)
            .update(word.amount + adder)
        }
        Future.successful(0)
      case None =>
        db run {
          entities += BiWord(bigram, adder)
        }
    }
  }
}

