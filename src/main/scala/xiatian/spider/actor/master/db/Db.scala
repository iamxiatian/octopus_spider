package xiatian.spider.actor.master.db

import scala.util.Try

/**
  * 数据库特质
  */
trait Db {
  def open()

  def close()

  /**
    * 清空数据库中的数据
    */
  def clear: Try[Unit] = throw new NotImplementedError("clear method was not implemented.")

  /**
    * 修复存在错误的数据，默认不执行任何操作
    */
  def repair = {}
}
