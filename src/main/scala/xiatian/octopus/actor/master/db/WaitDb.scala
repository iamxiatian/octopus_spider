package xiatian.octopus.actor.master.db

import java.io.File

import org.joda.time.DateTime
import xiatian.octopus.common.MyConf
import xiatian.octopus.model.{FetchLink, FetchTask}
import xiatian.octopus.util.{HashUtil, HexBytesUtil}

/**
  * 等待数据库：因调度和更新策略，链接尚未达到可以被调度抓取的时间，此时存放在该数据库中。
  * 由注入程序调度该数据库，把达到抓取时间要求的链接从该数据库转存到爬行数据库CrawlDb中。
  *
  * @author Tian Xia
  * @param path
  */
class WaitDb(path: String) extends SortedSetDb("WaitDB", path) {
  
  /**
    * 把链接link压入到等待队列中，自动根据重试次数和刷新间隔计算下次抓取时间.
    * 实际刷新间隔采用了基于重试次数的二进制指数退避算法
    *
    * @param link 要压入的链接
    */
  def push(link: FetchLink): Unit = FetchTask.get(link) map {
    task =>
      task.nextFetchSeconds(link).map {
        interval =>
          //System.currentTimeMillis() / 1000 + link.`type`.refreshInSeconds * (Math.pow(2, link.retries)).toLong
          val nextFetchTime = System.currentTimeMillis() / 1000 + interval
          push(link, nextFetchTime, ScoreUpdate.LT_OLD)
      }
  }

  /**
    * 向waitingDb中加入一个链接, 下次更新时间为nextFetchInSeconds
    *
    * @param link
    * @param nextFetchInSeconds
    */
  def push(link: FetchLink,
           nextFetchInSeconds: Long,
           strategy: ScoreUpdate = ScoreUpdate.LT_OLD): Unit = {
    val key = link.urlHash
    val value = link.toBytes()
    //时间戳比数据库中的原时间戳更小，才会更新时间戳(nextFetchInSeconds)
    put(key, value, nextFetchInSeconds, strategy)
  }

  def removeByUrl(url: String): Unit = remove(HashUtil.hashAsBytes(url))

  /**
    * 从数据库中弹出已经到达抓取时间的至多topN个抓取链接
    *
    * @param topN
    * @return
    */
  def popCrawlLinks(topN: Int): Seq[FetchLink] = {
    val seconds = System.currentTimeMillis() / 1000
    popTopList(seconds, topN).map {
      case (key, value, score) =>
        FetchLink.readFrom(value)
    }
  }

  /**
    * 获取指定分页内的链接数据
    */
  def pageFetchLinks(page: Int, pageSize: Int): Seq[(FetchLink, DateTime)] =
    pageList(page, pageSize).map {
      case (key, value, score) =>
        (FetchLink.readFrom(value), new DateTime(score * 1000L))
    }

  /**
    * 将当前的URL的调度时间，修改为指定的时间
    *
    * @param urlHashHex URL哈希后的十六进制表示的字符串
    */
  def promoteTime(urlHashHex: String, fetchTime: DateTime): Boolean = {
    val key = HexBytesUtil.hex2bytes(urlHashHex)
    modifyScore(key, fetchTime.getMillis / 1000)
  }
}

object WaitDb extends
  WaitDb(new File(MyConf.masterDbPath, "waiting").getCanonicalPath) {

}
