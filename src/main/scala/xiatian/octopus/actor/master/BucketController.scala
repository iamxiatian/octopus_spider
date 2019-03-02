package xiatian.octopus.actor.master

import java.util.concurrent._

import org.slf4j.LoggerFactory
import xiatian.octopus.common.MyConf
import xiatian.octopus.model.{FetchLink, FetchType}
import xiatian.octopus.util.HashUtil

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Random

/**
  * 抓取任务的桶控制器， 负责从链接中心LinkCenter中读取待抓取的URL，
  * 按域名散列注入到桶中，以避免单个域名
  * 有多个爬虫同时抓取导致目标网站服务器压力过大。
  *
  * 向桶中注入链接、从桶中提取URL进行抓取的任务异步进行，避免阻塞.
  *
  * 问题： urlHashes会不会有线程同步和内存泄漏问题？
  *
  * @author Tian Xia
  *         Dec 05, 2016 00:11
  */
object BucketController extends MasterConfig {
  val log = LoggerFactory.getLogger("BucketController")

  val picker: BucketPicker =
    MyConf.getString("master.bucket.picker", "simple").toLowerCase match {
      case "advanced" => AdvancedPicker
      case "random" => RandomPicker
      case _ => SimplePicker
    }

  /**
    * Master维持的桶对象，每个桶里面存放了一定数量的抓取链接，
    */
  val buckets: Seq[Bucket] = (0 until numberOfBuckets).map(idx => new Bucket(idx, maxBucketSize))
  //    if (MyConf.getString("master.bucket.picker", "simple") == "advanced")
  //      AdvancedPicker
  //    else
  //      SimplePicker
  /**
    * 控制抓取速度的Map，主键为爬虫的主机地址+任务的频道ID，值为以毫秒为单位的最近更新时间戳
    * 值为最近一次下发该频道任务的时间
    * 只有当频道的reqTimeInterval设置大于0时，才起作用。
    */
  val boardSpeedMillisRequestMap = new ConcurrentHashMap[String, Long]()
  /**
    * 控制抓取速度，该Map中维持了频道ID和reqTimeInterval的对应关系,
    * 该Map主键和值的对应数值，在FetchMasterActor获取链接的上下文Context对象时，
    * 隐藏在了Board.context(boardId)方法内部。
    * Map中的主键为频道ID，值为以秒为单位的时间间隔
    */
  val boardSpeedControlMap = new ConcurrentHashMap[String, Int]()
  /**
    * 控制抓取速度的Map，主键为主机地址，值为最近一次下发该频道任务的时间
    */
  val domainSpeedMillisRequestMap = new ConcurrentHashMap[String, Long]()
  /**
    * 桶中拥有的URL哈希值,记录了URL哈希值和
    */
  val urlHashes = new ConcurrentHashMap[Long, Long]()
  val fiveHours: Long = 1000L * 60 * 60 * 5
  var IS_STOPPING = false //是否正在终止程序,如果是，则不再注入链接或者提供链接抓取
  var lastFillTime = System.currentTimeMillis()

  /**
    * 获取一个抓取任务，策略如下：
    * （1）如果桶内数据为空，返回None
    * （2）不都为空的情况下，以概率选择某种类型的链接
    *
    * @param fetcherHost 爬虫所在的计算机地址/名称
    * @param fetcherId
    * @return
    */
  def getFetchLink(fetcherHost: String, fetcherId: Int): Option[FetchLink] =
    if (IS_STOPPING)
      None
    else {
      val bucket = getFetcherBucket(fetcherId)

      bucket.pop() match {
        case Some(link) =>
          // 先按照域名进行全局限速判断
          val host = link.getHost.toLowerCase
          if (MyConf.speedControlMap.containsKey(host)) {
            //判断是否需要根据域名进行限速
            val intervalMillis = {
              val item = MyConf.speedControlMap.getOrDefault(host, (0, 0))
              val gap: Int = item._2 - item._1

              //如果间隔大于0，则生成一个随机数，和最小数值相加，作为实际的限速数值
              //否则直接取第一个数值
              if (gap > 0)
                item._1 + Random.nextInt(gap + 1)
              else
                item._1
            }

            val lastRequestTimeMillis =
              domainSpeedMillisRequestMap.getOrDefault(host, 0)

            if (intervalMillis == 0)
              Some(link) //不需要限速
            else if ((System.currentTimeMillis - lastRequestTimeMillis) > intervalMillis) {
              //更新时间戳，下发任务
              domainSpeedMillisRequestMap.put(host, System.currentTimeMillis())
              Some(link)
            } else {
              //未到时间，不抓取该任务，该任务重新加入队列
              bucket.putLast(link)
              print("⏰") //限速符号⏰
              None
            }
          } else {
            //判断任务页面是否限制速度
            val intervalMillis =
              boardSpeedControlMap.getOrDefault(link.taskId, 0) * 1000

            val key = fetcherHost + "@" + link.taskId

            val lastRequestTimeMillis =
              boardSpeedMillisRequestMap.getOrDefault(key, 0)

            if (intervalMillis == 0)
              Some(link) //不需要限速
            else if ((System.currentTimeMillis - lastRequestTimeMillis) > intervalMillis) {
              //更新时间戳，下发任务
              boardSpeedMillisRequestMap.put(key, System.currentTimeMillis())
              Some(link)
            } else {
              bucket.putLast(link) //未到时间，不抓取该任务，该任务重新加入队列
              print("\uD83C\uDF75") //限速符号 🍵
              None
            }
          }
        case None =>
          print("\uD83C\uDE33") //空任务符号： 🈳
          None
      }
    }

  /**
    * 获取指定爬虫的链接任务所在的桶队列, 如果爬虫所对应的桶没有要抓取的链接，
    * 则自动采用第一个拥有链接的桶
    *
    * @param fetcherId
    * @return
    */
  private def getFetcherBucket(fetcherId: Int): Bucket = {
    val best = buckets(fetcherId % numberOfBuckets)
    if (best.isEmpty) {
      //取到第一个有数据的任务
      val candidate = buckets.dropWhile(c => c.isEmpty).headOption
      if (candidate.isEmpty) best else candidate.get
    } else best
  }

  /**
    * 获取所有桶内的所有链接的数量
    */
  def totalLinkCount(): Int = buckets.map(_.count).sum

  /**
    * 获取每一个桶当前的所有链接数量
    */
  def eachBucketLinkCount(): Seq[Int] = buckets.map(_.count)

  /**
    * 如果桶已经存在该URL，返回true，表示注入成功。
    *
    * 如果没有在桶中，且highPriority = false: 则把当前链接尝试加入桶中队列的末尾，
    * 如果桶已经满了，则返回false，
    *
    * 如果没有在桶中，且highPriority = true，则插入到队列的开始，返回true
    *
    * @param link
    * @param highPriority 如果为true，则插入到队列的尾部，方便快速出队列
    * @return
    */
  def fillLink(link: FetchLink, highPriority: Boolean = false): Boolean =
    if (IS_STOPPING)
      false
    else if (inBucket(link))
      true
    else {
      val bucket = picker.pick(link) //选择要注入的桶
      if (bucket.count(link.`type`) < maxBucketSize || highPriority) {
        markInBucket(link)

        if (highPriority) {
          if (bucket.count(link.`type`) == maxBucketSize) {
            //把队尾的数据链接回压到链接中心库中
            UrlManager.pushLinkBack(bucket.takeLast(link.`type`))
          }
          bucket.putFirst(link)
        } else {
          bucket.putLast(link)
        }
        true
      } else {
        //不注入到桶中
        false
      }
    }

  def markInBucket(link: FetchLink) =
    urlHashes.put(hashLong(link.url), System.currentTimeMillis())

  def inBucket(link: FetchLink) = {
    val key = hashLong(link.url)
    if (urlHashes.containsKey(key)) {
      val lastTimeMillis: Long = urlHashes.getOrDefault(key, 0L)
      //如果当前时间比上次时间小于100分钟，则返回在桶内，
      if ((System.currentTimeMillis() - lastTimeMillis) < fiveHours) {
        true
      } else {
        //否则，当过期处理, 返回不在桶内
        log.debug(s"Bucket hash key expired for ${link.url}")
        urlHashes.remove(key)
        false
      }
    } else false
  }

  def hashLong(s: String) = HashUtil.hashAsLong(s)

  /**
    * 向桶中注入链接，返回注入的不同类型的链接的名称和数量, List中的每一个数字对应于LinkType.all
    * 中的每一个链接类型
    */
  def fillBuckets(): Future[Seq[(String, Int)]] = Future.successful {
    if (!IS_STOPPING && (System.currentTimeMillis() - lastFillTime) > 5000) {
      //更新最近注入时间
      lastFillTime = System.currentTimeMillis()

      //尝试注入每一个种类的链接
      FetchType.all.map {
        linkType =>
          //避免其他的在短时间内再次进入
          val total = totalLinkCount(linkType)

          //内存队列中的数量不到最大总数量的4/5时，才尝试注入
          val filledCount: Int =
            if (total < (maxBucketSize * numberOfBuckets) * 4.0 / 5.0)
              fillBuckets(linkType, bucketLinkFillSize)
            else 0

          //继续更新最近注入时间
          lastFillTime = System.currentTimeMillis()
          (linkType.name, filledCount)
      }
    } else {
      FetchType.all.map(t => (t.name, 0))
    }
  }

  /**
    * 获取所有桶内的指定类型的链接数量
    */
  def totalLinkCount(t: FetchType): Int = buckets.map(_.count(t)).sum

  /**
    * 把num个链接向桶里面填充, 返回填充成功的数量
    */
  private def fillBuckets(t: FetchType, num: Int): Int

  =
    if (IS_STOPPING) 0
    else (0 until Math.min(num, UrlManager.queueSize(t)))
      .flatMap(_ => UrlManager.popLink(t))
      .map {
        link =>
          //选择要注入链接的桶
          val bucket = picker.pick(link)
          val currentSize = bucket.count(t)

          if (!inBucket(link)) {
            if (currentSize > maxBucketSize) {
              //重新加到队列的末尾，延迟被再次抓取的时间
              UrlManager.pushLink(link)
              0
            } else {
              markInBucket(link)
              bucket.putLast(link)
              1 //成功注入一条链接，计数为1
            }
          } else {
            log.debug(s"${link.url} has already in bucket, skip it.")
            0
          }
      }.sum

  def removeFromBucket(link: FetchLink) =
    urlHashes.remove(hashLong(link.url))

  /**
    * 归还内存中的链接到Redis数据库中
    */
  def returnLinks(): Future[Long] = {
    IS_STOPPING = true
    println("Try to return bucket links...")

    Future.sequence(
      (0 until numberOfBuckets).map {
        idx =>
          val bucket = buckets(idx)
          println(s"\tpush link back for bucket $idx ...")

          val total = bucket.returnLinks

          println(s"  returned $total links in bucket $idx")
          Future.successful(total)
      }
    ).flatMap(counts => Future.successful(counts.sum))
  }

  /**
    * 直接清空内存桶里面的数据
    */
  def empty(): Unit = {
    (0 until numberOfBuckets).foreach {
      idx =>
        val bucket = buckets(idx)
        bucket.clear
    }

    urlHashes.clear
  }

}
