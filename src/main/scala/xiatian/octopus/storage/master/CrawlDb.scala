package xiatian.octopus.storage.master

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap

import org.slf4j.LoggerFactory
import xiatian.octopus.common.MyConf
import xiatian.octopus.model.{FetchItem, FetchType}
import xiatian.octopus.storage.Db
import xiatian.octopus.storage.ast.QueueMapDb
import xiatian.octopus.task.FetchTask

import scala.util.{Failure, Success, Try}

/**
  * 需要抓取的爬行队列库，该队列中的URL都已经到了抓取时间，但由于系统资源限制，
  * 还没有被调度到内存之中。
  */
private class CrawlDb(path: String, capacity: Int = 100000000)
  extends QueueMapDb(path, capacity) {

  def pushLink(FetchItem: FetchItem) =
    enqueue(
      FetchItem.url.getBytes(StandardCharsets.UTF_8),
      FetchItem.toBytes
    )

  def popLink(): Option[FetchItem] = dequeue().map(FetchItem.readFrom(_))

  def contains(url: String) = containsKey(url.getBytes(StandardCharsets.UTF_8))

  def returnLink(FetchItem: FetchItem) =
    returnQueue(
      FetchItem.url.getBytes(StandardCharsets.UTF_8),
      FetchItem.toBytes
    )
}

object CrawlDb extends Db {
  private val crawlPath = new File(MyConf.masterDbPath, "crawl")

  private val LOG = LoggerFactory.getLogger("CrawlDb")

  //create crawl path if not exists.
  crawlPath.mkdirs()

  private val crawlDbs: ConcurrentHashMap[Int, CrawlDb] = new ConcurrentHashMap
  private val signatureDbs: ConcurrentHashMap[Int, SignatureDb] = new ConcurrentHashMap

  def open() = {
    println(s"crawl db and signature db will be opened dynamically.")
  }

  def pushLink(link: FetchItem) = Try {
    getCrawlDb(link.`type`).pushLink(link)
    getSignatureDb(link.`type`).put(link.urlHash)
  } match {
    case Success(_) => true
    case Failure(e) =>
      LOG.error(e.toString)
      false
  }

  def returnLink(link: FetchItem) = Try {
    getCrawlDb(link.`type`).returnLink(link)
    getSignatureDb(link.`type`).put(link.urlHash)
  } match {
    case Success(_) => true
    case Failure(e) =>
      LOG.error(e.toString)
      false
  }

  private def getCrawlDb(t: FetchType): CrawlDb = {
    val id = t.id
    if (crawlDbs.contains(id)) {
      crawlDbs.get(id)
    } else {
      //双重锁机制
      synchronized {
        val oldDb = crawlDbs.get(id)
        if (oldDb == null) {
          val db = new CrawlDb(new File(crawlPath, s"queue.$id").getCanonicalPath)
          crawlDbs.put(id, db)
          db
        } else {
          oldDb
        }
      }
    }
  }

  def popLink(t: FetchType) = {
    val link = getCrawlDb(t).popLink()
    if (link.nonEmpty) getSignatureDb(t).remove(link.get.urlHash)
    link
  }

  /**
    * URL是否已经在队列中了
    *
    * @param link
    * @return
    */
  def has(link: FetchItem): Boolean = {
    val expiredSeconds = FetchTask.get(link).map {
      task =>
        task.nextFetchSeconds(link).getOrElse(MyConf.MaxTimeSeconds)
    }.getOrElse(MyConf.MaxTimeSeconds)

    getSignatureDb(link.`type`).has(link.urlHash, expiredSeconds)
  }

  private def getSignatureDb(t: FetchType): SignatureDb = {
    val id = t.id
    if (signatureDbs.contains(id)) {
      signatureDbs.get(id)
    } else {
      //双重锁机制
      synchronized {
        val oldDb = signatureDbs.get(id)
        if (oldDb == null) {
          val db = new SignatureDb(
            s"crawl signature db($id)",
            new File(crawlPath, s"signature.$id"),
            2000)
          signatureDbs.put(id, db)
          db
        } else {
          oldDb
        }
      }
    }
  }

  def queueSize(t: FetchType) = getCrawlDb(t).count()

  def close(): Unit = {
    println("===Closing CRAWL DB===")
    crawlDbs.values().forEach(db => db.close())
    signatureDbs.values().forEach(db => db.close())
    println("[CRAWL DB CLOSED]\n")
  }
}
