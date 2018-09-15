package xiatian.spider.actor.master.db

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8

import com.google.common.collect.Lists
import com.google.common.primitives.Longs
import org.joda.time.DateTime
import org.rocksdb._
import org.zhinang.util.cache.LruCache
import xiatian.common.MyConf
import xiatian.spider.actor.master.db.BadLinkType.BadLinkType

import scala.util.Try

object BadLinkType extends Enumeration {
  type BadLinkType = Value
  val DeadUrl, UnknownHost = Value
}

/**
  * URL坏链库，包括不能访问的主机和无法访问的链接.
  *
  * 在RocksDB中，默认ColumnFamily存放了死链(Dead URL)，
  * 而"UnknownHost"列族存放了无法访问的主机(Unknown Host)
  *
  * @param path      书库库的保存路径
  * @param cacheSize 缓存大小
  */
class BadLinkDb(path: String,
                cacheSize: Int
               ) extends Db {


  private val options = new DBOptions()
    .setCreateIfMissing(true)
    .setCreateMissingColumnFamilies(true)


  //默认的Column Family下标位置
  private val DEAD_LINK_CF_IDX = 0;

  //元数据（Front，Rear等信息）Column Family下标位置
  private val UNKNOWN_HOST_CF_IDX = 1;

  private val cfNames = Lists.newArrayList[ColumnFamilyDescriptor](
    new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
    new ColumnFamilyDescriptor("UnknownHost".getBytes()) //队列元数据
  )

  private val cfHandlers = Lists.newArrayList[ColumnFamilyHandle]

  private val db = RocksDB.open(options, path, cfNames, cfHandlers)

  private val deadUrlHandler = cfHandlers.get(0)
  private val unknownHostHandler = cfHandlers.get(1)


  def numKeys() = db.getLongProperty("rocksdb.estimate-num-keys")

  def open() = {
    println(s"open BadLink db with count: ${numKeys()}")
  }

  /**
    * 数据库中有的数据的缓存
    */
  private val hitCache = new LruCache[String, Long](cacheSize)

  /**
    * 数据库中不存在数据的缓存，避免每次都进一步判断数据库是否有
    */
  private val notHitCache = new LruCache[String, Long](cacheSize)

  /**
    * 把URL保存到数据库中
    *
    * @param url
    */
  private def save(url: String, badType: BadLinkType): Unit = {
    val time = System.currentTimeMillis()
    badType match {
      case BadLinkType.DeadUrl =>
        db.put(deadUrlHandler, url.getBytes(UTF_8), Longs.toByteArray(time))
      case BadLinkType.UnknownHost =>
        db.put(unknownHostHandler, url.getBytes(UTF_8), Longs.toByteArray(time))
    }

    notHitCache.remove(url)
    hitCache.put(url, time)
  }

  def saveUrl(url: String): Unit = save(url, BadLinkType.DeadUrl)

  def saveHost(host: String): Unit = save(host, BadLinkType.UnknownHost)

  private def remove(url: String, badType: BadLinkType): Unit = {
    hitCache.remove(url)
    notHitCache.put(url, System.currentTimeMillis())

    badType match {
      case BadLinkType.DeadUrl =>
        db.delete(deadUrlHandler, url.getBytes(UTF_8))
      case BadLinkType.UnknownHost =>
        db.delete(unknownHostHandler, url.getBytes(UTF_8))
    }
  }

  def removeUrl(url: String): Unit = remove(url, BadLinkType.DeadUrl)

  def removeHost(host: String): Unit = remove(host, BadLinkType.UnknownHost)

  private def getFromDb(url: String, badType: BadLinkType): Option[Long] = {
    val value = badType match {
      case BadLinkType.DeadUrl => db.get(deadUrlHandler, url.getBytes(UTF_8))
      case BadLinkType.UnknownHost => db.get(unknownHostHandler, url.getBytes(UTF_8))
    }

    if (value == null) {
      notHitCache.put(url, System.currentTimeMillis())
      None
    } else {
      val time = Longs.fromByteArray(value)

      //add to cache
      hitCache.put(url, time)
      notHitCache.remove(url)

      Some(time)
    }
  }

  /**
    * 库里面是否保存有url
    *
    * @param url
    * @return
    */
  private def has(url: String, badType: BadLinkType): Boolean = {
    if (hitCache.containsKey(url))
      true
    else if (notHitCache.containsKey(url))
      false
    else getFromDb(url, badType).nonEmpty
  }

  def hasHost(host: String): Boolean = has(host, BadLinkType.UnknownHost)

  def hasUrl(url: String): Boolean = has(url, BadLinkType.DeadUrl)

  /**
    * 库里面是否在expiredSeconds之内保存有该url
    *
    * @param url
    * @param expiredSeconds
    * @return
    */
  private def has(url: String, badType: BadLinkType, expiredSeconds: Long): Boolean = {
    val timeInCache = hitCache.get(url)

    if (notHitCache.containsKey(url))
      false
    else if (timeInCache > 0) { //对于Long，如果cache不存在对应的主键，则返回0
      val cacheValid = (System.currentTimeMillis() - expiredSeconds * 1000) < timeInCache
      if (!cacheValid) {
        //删除失效数据
        remove(url, badType)
      }
      cacheValid
    } else {
      val timeInDb = getFromDb(url, badType)
      if (timeInDb.isEmpty) {
        false
      } else {
        val valid = (System.currentTimeMillis() - expiredSeconds * 1000) < timeInDb.get
        if (!valid) {
          //删除过期的数据
          remove(url, badType)
        }
        valid
      }
    }
  }

  def hasHost(host: String, expiredSeconds: Long): Boolean =
    has(host, BadLinkType.UnknownHost, expiredSeconds)

  def hasUrl(url: String, expiredSeconds: Long): Boolean =
    has(url, BadLinkType.DeadUrl, expiredSeconds)

  def close() = {
    println(s"===Close BadLink DB=== \n\t $path ...")

    cfHandlers.forEach(_.close)
    if (db != null) db.close
    if (options != null) options.close

    println("  [BadLink DB CLOSED]\n")
  }

  override def clear: Try[Unit] = Try {
    val it = db.newIterator()
    it.seekToFirst()
    while (it.isValid) {
      db.delete(it.key())
      it.next()
    }
    it.close()
    db.compactRange()
  }

  /**
    * 返回topN条错误链接记录，每一个二元组保存了(url, date)
    *
    * @param topN
    * @return
    */
  def listDeadUrls(topN: Int): List[(String, String)] = {
    val it = db.newIterator(deadUrlHandler)
    it.seekToFirst()

    val result = for (_ <- 0 until topN; if it.isValid)
      yield {
        val key = it.key()
        val time = Longs.fromByteArray(it.value())
        it.next()
        (new String(key, UTF_8), new DateTime(time).toString("yyyy-MM-dd HH:mm:ss"))
      }

    it.close()
    result.toList
  }
}

object BadLinkDb extends BadLinkDb(
  new File(MyConf.masterDbPath, "badurl").getCanonicalPath,
  MyConf.masterDbCacheSize * 2
)