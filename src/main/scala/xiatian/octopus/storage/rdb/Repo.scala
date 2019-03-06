package xiatian.octopus.storage.rdb

import org.slf4j.LoggerFactory
import slick.jdbc.MySQLProfile
import xiatian.octopus.common.MyConf
import xiatian.octopus.task.epaper.EPaperArticleDb

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

/**
  * 处理数据的Repository, 目前支持Mysql
  *
  * @tparam T
  */
trait Repo[T] {
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

  val profile = MySQLProfile

  val db = RepoProvider.mysqlDb

  val LOG = LoggerFactory.getLogger(this.getClass)
}

object RepoProvider {

  import slick.jdbc.MySQLProfile.api._

  val mysqlDb: Database = Database.forConfig("store.db.mysql", MyConf.config)

  def close(): Unit = {
    if (mysqlDb != null)
      mysqlDb.close()
  }
}

object Repo extends App {
  //@TODO 改成命令行参数
  Await.result(EPaperArticleDb.createSchema, Duration.Inf)
}
