package xiatian.octopus.actor.master

import java.util.concurrent.LinkedBlockingDeque

import xiatian.octopus.model.{FetchItem, FetchType}

import scala.collection.convert.ImplicitConversions._
import scala.util.Random

class FetchQueue(name: String) {
  private val q = new LinkedBlockingDeque[FetchItem]()

  def apply(name: String) = new FetchQueue(name)

  def clear(): Unit = q.clear()

  def putFirst(link: FetchItem): Unit = q.putFirst(link)

  def putLast(link: FetchItem): Unit = q.putLast(link)

  def takeFirst(): FetchItem = q.takeFirst()

  def takeLast(): FetchItem = q.takeLast()

  def links: List[FetchItem] = q.toList

  def nonEmpty: Boolean = !isEmpty

  def isEmpty: Boolean = q.isEmpty

  def lessThan(maxSize: Int, ratio: Double): Boolean = size <= (maxSize * ratio)

  def size: Int = q.size()

  def moreThan(maxSize: Int, ratio: Double): Boolean = size >= (maxSize * ratio)

  def pushLinkBack(): Int = q.map(l => if (UrlManager.pushLinkBack(l)) 1 else 0).sum
}

/**
  * 每个桶里面维持若干队列，每一种类型的链接都会放入到同一个队列中
  *
  * @param idx 桶的编号
  */
class Bucket(val idx: Int, val maxSize: Int) {

  import java.util.concurrent.ConcurrentHashMap

  /**
    * 主键为链接类型的ID，值为一个抓取队列
    */
  val queues = new ConcurrentHashMap[Int, FetchQueue]()

  def putLast(link: FetchItem): Unit = getQueue(link.`type`).putLast(link)

  def putFirst(link: FetchItem): Unit = getQueue(link.`type`).putFirst(link)

  def takeLast(t: FetchType): FetchItem = getQueue(t).takeLast()

  private def getQueue(t: FetchType): FetchQueue =
    if (queues.containsKey(t.id)) {
      queues.get(t.id)
    } else {
      val q = new FetchQueue(t.name)
      queues.put(t.id, q)
      q
    }

  def getLinks(t: FetchType): Seq[FetchItem] = getQueue(t).links

  /**
    * 返回一个链接，策略：
    * （1）如果桶内数据为空，返回None
    * （2）不都为空的情况下，以概率选择某种类型的链接
    *
    * @return
    */
  def pop(): Option[FetchItem] =
    if (isEmpty)
      None
    else {
      val types: List[FetchType] = queues.filter(_._2.nonEmpty).map {
        case (typeId, _) => FetchType(typeId)
      }.toList

      // priority加1避免0溢出
      val total = types.map(_.priority).sum

      //生成一个随机数，根据随机数选择对应的队列
      val rand = Random.nextInt().abs % (total + 1)

      //选择第一个LinkType，该LinkType的重要性和之前的累加值超过了随机数
      def locate(accumulator: Int, typeList: List[FetchType]): Option[FetchType] =
        typeList match {
          case Nil => Option.empty[FetchType]
          case h :: Nil => Some(h)
          case h :: tails =>
            if ((accumulator + h.priority) >= rand)
              Some(h)
            else
              locate(accumulator + h.priority, tails)
        }

      locate(0, types).map(takeFirst)
    }

  def takeFirst(t: FetchType): FetchItem = getQueue(t).takeFirst()

  def isEmpty: Boolean = queues.values().forall(_.isEmpty)

  def nonEmpty: Boolean = queues.values().forall(_.nonEmpty)

  def clear: Unit = queues.values().foreach(_.clear())

  def returnLinks: Int = queues.map { case (_, q) => q.pushLinkBack() }.sum

  def count: Int = FetchType.all.map(t => count(t)).sum

  def count(t: FetchType): Int = getQueue(t).size

  override def toString: String = {
    val numbers = queues.values().map(_.size).sum
    s"bucket: idx=$idx, total links=$numbers"
  }
}
