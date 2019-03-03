package xiatian.octopus.extract

import xiatian.octopus.actor.ProxyIp
import xiatian.octopus.model.FetchItem

import scala.concurrent.Future

case class ExtractResult(link: FetchItem, //要抽取的链接
                         childLinks: List[FetchItem] = List.empty[FetchItem], //抽取出来的子链接
                         datum: Map[String, Any] = Map.empty[String, Any], // 抽取出来的数据
                         proxyHolder: Option[ProxyIp] = None //抓取采用的代理, 当需要进一步采集信息时使用
                        ) {
  def isEmpty: Boolean = childLinks.isEmpty && datum.isEmpty

  def getString(key: String, default: String = ""): String =
    datum.getOrElse(key, default).asInstanceOf[String]

  def getInt(key: String, default: Int = 0): Int =
    datum.getOrElse(key, default).asInstanceOf[Int]

  def getFloat(key: String, default: Float = 0) =
    datum.getOrElse(key, default).asInstanceOf[Float]

  def getDouble(key: String, default: Double = 0): Double =
    datum.getOrElse(key, default).asInstanceOf[Double]

  def getList[T](key: String, default: List[T] = List.empty[T]): List[T] =
    datum.getOrElse(key, default).asInstanceOf[List[T]]

  def getOption[T](key: String, default: Option[T] = None): Option[T] =
    datum.getOrElse(key, default).asInstanceOf[Option[T]]

  /**
    * 保存抽取结果
    *
    * @return
    */
  def save(): Future[Boolean] = ???
}