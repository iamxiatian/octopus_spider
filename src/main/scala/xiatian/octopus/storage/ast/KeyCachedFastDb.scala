package xiatian.octopus.storage.ast

import org.rocksdb.{DBOptions, Options, RocksDB, RocksIterator}
import org.zhinang.util.cache.LruCache
import xiatian.octopus.storage.Db

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

/**
  * 所有key都放在缓存中的数据库
  *
  * 以列表方式存放一系列元素的数据库，实现时把所有的主键读到内存中缓存起来，方便遍历。
  * 同时，内部维持了一个LRUCache，缓存经常被访问的主键对应的value。
  *
  * @param path      数据库的保存路径
  * @param cacheSize 缓存大小
  */
class KeyCachedFastDb(path: String, cacheSize: Int = 2000) extends Db {
  RocksDB.loadLibrary()
  println(s"OPEN>>>>>>>>>>>>>>>>>>>>>>>")

//  val options = new DBOptions()
//    .setCreateIfMissing(true)
//    .setCreateMissingColumnFamilies(true)

  private val options = new Options().setCreateIfMissing(true)
  private val db = RocksDB.open(options, path)

  println(s"OPEN $path with count $numKeys")

  /**
    * 数据库中有的数据的缓存
    */
  private val hitCache = new LruCache[Array[Byte], Array[Byte]](cacheSize)

  lazy val keys: mutable.SortedSet[Array[Byte]] = {
    implicit val o = new math.Ordering[Array[Byte]] {
      def compare(a: Array[Byte], b: Array[Byte]): Int = {
        if (a eq null) {
          if (b eq null) 0
          else -1
        }
        else if (b eq null) 1
        else {
          val L = math.min(a.length, b.length)
          var i = 0
          while (i < L) {
            if (a(i) < b(i)) return -1
            else if (b(i) < a(i)) return 1
            i += 1
          }
          if (L < b.length) -1
          else if (L < a.length) 1
          else 0
        }
      }
    }

    val sortedSet = mutable.SortedSet.empty[Array[Byte]]
    val iter: RocksIterator = db.newIterator
    iter.seekToFirst()

    while (iter.isValid) {
      sortedSet += iter.key()
      iter.next()
    }
    sortedSet
  }


  def count() = keys.size


  /**
    * 保存到数据库中
    */
  def write(key: Array[Byte], value: Array[Byte]): KeyCachedFastDb = {
    db.put(key, value)

    keys += key
    hitCache.put(key, value)

    this
  }

  def read(key: Array[Byte]): Option[Array[Byte]] = {
    val cachedValue = hitCache.get(key)

    if (cachedValue == null) {
      readFromDb(key)
    } else {
      Some(cachedValue)
    }
  }

  private def readFromDb(key: Array[Byte]): Option[Array[Byte]] = {
    val value = db.get(key)
    if (value == null) {
      None
    } else {
      //add to cache
      hitCache.put(key, value)

      Some(value)
    }
  }

  /**
    * 库里面是否保存有key
    *
    */
  def has(key: Array[Byte]): Boolean = keys.contains(key)

  /**
    * 获取元素的主键和对应的值
    *
    * @param start 开始位置，第一个元素的下标为0
    * @param count 从开始位置计算，保留的元素数量
    * @return
    */
  def elements(start: Int, count: Int): Map[Array[Byte], Array[Byte]] = {
    val partKeys: List[Array[Byte]] = keys.drop(start).take(count).toList

    db.multiGet(partKeys.asJava).asScala.toMap
  }

  override def clear: Try[Unit] = Try {
    keys.toSeq.foreach(remove)
  }

  def remove(key: Array[Byte]): KeyCachedFastDb = {
    keys -= key
    db.delete(key)
    hitCache.remove(key)

    this
  }

  def open() = {
    println(s"open board db with count: ${numKeys()}")
  }

  def numKeys() = db.getLongProperty("rocksdb.estimate-num-keys")

  def close() = {
    println(s"===Close board db === \n\t $path ...")
    db.close()
    options.close()
    println("  [BOARD DB CLOSED]\n")
  }
}
