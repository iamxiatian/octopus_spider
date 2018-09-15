package xiatian.spider.parse

import scala.concurrent.Future

/**
  * 保存抽取后的数据
  */
trait Saver {
  def save(result: ExtractResult): Future[Boolean]
}

case object EmptySaver extends Saver {
  def save(result: ExtractResult) = Future.successful {
    true
  }
}
