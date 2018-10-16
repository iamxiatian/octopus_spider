package xiatian.octopus.actor.master.db

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap

import org.slf4j.LoggerFactory
import xiatian.octopus.common.MyConf
import xiatian.octopus.model.{FetchLink, FetchTask, LinkType}

import scala.util.{Failure, Success, Try}

/**
  * 需要抓取的爬行队列库，该队列中的URL都已经到了抓取时间，但由于系统资源限制，还没有被调度到。
  */
class CrawlDb(path: String, capacity: Int = 100000000)
  extends QueueMapDb(path, capacity) {
  def pushLink(fetchLink: FetchLink) =
    enqueue(
      fetchLink.url.getBytes(StandardCharsets.UTF_8),
      fetchLink.toBytes
    )

  def popLink(): Option[FetchLink] = dequeue().map(FetchLink.readFrom(_))

  def contains(url: String) = containsKey(url.getBytes(StandardCharsets.UTF_8))

  def returnLink(fetchLink: FetchLink) =
    returnQueue(
      fetchLink.url.getBytes(StandardCharsets.UTF_8),
      fetchLink.toBytes
    )
}

object CrawlDb extends Db {
  private val crawlPath = new File(MyConf.masterDbPath, "crawl")

  private val LOG = LoggerFactory.getLogger("CrawlDb")

  //create crawl path if not exists.
  crawlPath.mkdirs()

  private val crawlDbs: ConcurrentHashMap[Int, CrawlDb] = new ConcurrentHashMap
  private val signatureDbs: ConcurrentHashMap[Int, SignatureDb] = new ConcurrentHashMap

  private def getCrawlDb(t: LinkType): CrawlDb = {
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

  private def getSignatureDb(t: LinkType): SignatureDb = {
    val id = t.id
    if (signatureDbs.contains(id)) {
      signatureDbs.get(id)
    } else {
      //双重锁机制
      synchronized {
        val oldDb = signatureDbs.get(id)
        if (oldDb == null) {
          val db = new SignatureDb(new File(crawlPath, s"signature.$id").getCanonicalPath, 2000)
          signatureDbs.put(id, db)
          db
        } else {
          oldDb
        }
      }
    }
  }

  def open() = {
    println(s"crawl db and signature db will be opened dynamically.")
  }

  def pushLink(link: FetchLink) = Try {
    getCrawlDb(link.`type`).pushLink(link)
    getSignatureDb(link.`type`).put(link.urlHash)
  } match {
    case Success(_) => true
    case Failure(e) =>
      LOG.error(e.toString)
      false
  }

  def returnLink(link: FetchLink) = Try {
    getCrawlDb(link.`type`).returnLink(link)
    getSignatureDb(link.`type`).put(link.urlHash)
  } match {
    case Success(_) => true
    case Failure(e) =>
      LOG.error(e.toString)
      false
  }

  def popLink(t: LinkType) = {
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
  def has(link: FetchLink): Boolean = {
    val expiredSeconds = FetchTask.get(link).map {
      task =>
        task.nextFetchSeconds(link).getOrElse(MyConf.MaxTimeSeconds)
    }.getOrElse(MyConf.MaxTimeSeconds)

    getSignatureDb(link.`type`).has(link.urlHash, expiredSeconds)
  }

  def queueSize(t: LinkType) = getCrawlDb(t).count()

  def close(): Unit = {
    println("===Closing CRAWL DB===")
    crawlDbs.values().forEach(db => db.close())
    signatureDbs.values().forEach(db => db.close())
    println("[CRAWL DB CLOSED]\n")
  }
}
