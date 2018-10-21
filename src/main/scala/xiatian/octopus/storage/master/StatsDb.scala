package xiatian.octopus.storage.master

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.ConcurrentHashMap

import com.google.common.primitives.Longs
import org.rocksdb._
import org.zhinang.util.cache.LruCache
import xiatian.octopus.common.MyConf
import xiatian.octopus.storage.Db

import scala.collection.JavaConverters._
import scala.concurrent.Future


/**
  * 记录URL统计信息的统计库，包括每天，每小时抓取的各类型的网页数量
  */
class StatsDb(path: String,
              cacheSize: Int = 3000
             ) extends Db {
  RocksDB.loadLibrary()

  private val options = new Options()
    .setCreateIfMissing(true)

  private val db = RocksDB.open(options, path)
  private val writeCache = new ConcurrentHashMap[String, Long]()
  private val hitCache = new LruCache[String, Long](cacheSize)
  private var lastSyncTime = System.currentTimeMillis()

  def open() = {
    println(s"open statistical db with count: ${numKeys()}")
  }

  def numKeys() = db.getLongProperty("rocksdb.estimate-num-keys")

  def inc(key: String, adder: Long): Unit = {
    writeCache.put(key, writeCache.getOrDefault(key, 0) + adder)

    //15分钟同步一次写数据:15*60*1000=900 000
    if (lastSyncTime + 900000 < System.currentTimeMillis()) {
      lastSyncTime = System.currentTimeMillis()
      Future.successful(flush())
    }
  }

  def flush(): Unit = {
    writeCache.asScala.foreach {
      case (k: String, v: Long) =>
        val key = k.getBytes(UTF_8)
        val value = db.get(key)
        if (value == null) {
          db.put(key, Longs.toByteArray(v))
        } else {
          db.put(key, Longs.toByteArray(v + Longs.fromByteArray(value)))
        }
    }
    writeCache.clear()
  }

  def get(key: String): Long = {
    if (hitCache.containsKey(key)) {
      hitCache.get(key) + writeCache.getOrDefault(key, 0)
    } else {
      getFromDb(key) + writeCache.getOrDefault(key, 0)
    }
  }

  private def getFromDb(key: String): Long = {
    val value = db.get(key.getBytes(UTF_8))
    if (value == null) {
      0
    } else {
      val cnt = Longs.fromByteArray(value)

      //add to cache
      hitCache.put(key, cnt)

      cnt
    }
  }

  def close() = {
    println(s"===Closing Statistical DB=== \n\t $path ...")

    flush()
    if (db != null) db.close
    if (options != null) options.close

    println("  [Statistical DB CLOSED]\n")
  }
}

object StatsDb extends StatsDb(
  new File(MyConf.masterDbPath, "stats").getCanonicalPath,
  2000
) {

}