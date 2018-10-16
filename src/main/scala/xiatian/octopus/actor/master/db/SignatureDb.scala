package xiatian.octopus.actor.master.db

import java.io.File

import com.google.common.primitives.Longs
import org.rocksdb.{Options, RocksDB}
import org.zhinang.util.cache.LruCache
import xiatian.octopus.common.MyConf
import xiatian.octopus.util.HexBytesUtil

import scala.util.Try

/**
  * URL的签名库，通过签名记录已经采集过的特定*
  *
  * @param path      书库库的保存路径
  * @param cacheSize 缓存大小
  */
class SignatureDb(path: String,
                  cacheSize: Int) extends Db {
  RocksDB.loadLibrary()

  private val options = new Options().setCreateIfMissing(true)
  private val db = RocksDB.open(options, path)

  def numKeys() = db.getLongProperty("rocksdb.estimate-num-keys")

  def open() = {
    println(s"Signature db opened (${numKeys()} keys)")
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
    * 把Hash保存到数据库中
    */
  def put(hash: Array[Byte]): Unit = {
    val time = System.currentTimeMillis()
    db.put(hash, Longs.toByteArray(time))

    val hex = HexBytesUtil.bytes2hex(hash)
    notHitCache.remove(hex)
    hitCache.put(hex, time)
  }


  def remove(hash: Array[Byte]): Unit = {
    val hex = HexBytesUtil.bytes2hex(hash)
    hitCache.remove(hex)
    notHitCache.put(hex, System.currentTimeMillis())
    db.delete(hash)
  }

  private def getFromDb(hash: Array[Byte]): Option[Long] = {
    val value = db.get(hash)
    val hex = HexBytesUtil.bytes2hex(hash)
    if (value == null) {
      notHitCache.put(hex, System.currentTimeMillis())
      None
    } else {
      val time = Longs.fromByteArray(value)

      //add to cache
      hitCache.put(hex, time)
      notHitCache.remove(hex)

      Some(time)
    }
  }

  /**
    * 库里面是否保存有该Hash
    *
    */
  def has(hash: Array[Byte]): Boolean = {
    val hex = HexBytesUtil.bytes2hex(hash)

    if (hitCache.containsKey(hex))
      true
    else if (notHitCache.containsKey(hex))
      false
    else getFromDb(hash).nonEmpty
  }

  /**
    * 库里面是否在expiredSeconds之内保存有该url
    *
    * @param hash
    * @param expiredSeconds
    * @return
    */
  def has(hash: Array[Byte], expiredSeconds: Long): Boolean = {
    val hex = HexBytesUtil.bytes2hex(hash)

    val timeInCache = hitCache.get(hex)

    if (notHitCache.containsKey(hex))
      false
    else if (timeInCache > 0) { //对于Long，如果cache不存在对应的主键，则返回0
      val cacheValid = (System.currentTimeMillis() - expiredSeconds * 1000) < timeInCache
      if (!cacheValid) {
        //删除失效数据
        remove(hash)
      }
      cacheValid
    } else {
      val timeInDb = getFromDb(hash)
      if (timeInDb.isEmpty) {
        false
      } else {
        val valid = (System.currentTimeMillis() - expiredSeconds * 1000) < timeInDb.get
        if (!valid) {
          //删除过期的数据
          remove(hash)
        }
        valid
      }
    }
  }

  def close() = {
    println(s"  ---Close database--- \n\t $path ...")
    db.close()
    println("  ---DONE---")
  }

  override def clear: Try[Unit] = Try {
    val it = db.newIterator()
    it.seekToFirst()
    while (it.isValid) {
      db.delete(it.key())
      it.next()
    }
    db.compactRange()
  }
}

object SignatureDb extends Db {
  //创建数据库目录
  private val signaturePath = new File(MyConf.masterDbPath, "signature")
  signaturePath.mkdirs()

  private val fetchedPath = new File(signaturePath, "fetched").getCanonicalPath

  private val fetchingPath = new File(signaturePath, "fetching").getCanonicalPath

  private val cacheSize = MyConf.masterDbCacheSize
  val fetchedDb = new SignatureDb(fetchedPath, cacheSize)
  val fetchingDb = new SignatureDb(fetchingPath, 2000)

  def open() = {
    println(s"fetched page signature count: ${fetchedDb.numKeys()}")
    println(s"fetching pages signature count: ${fetchingDb.numKeys()}")
  }

  def close() = {
    println("===Closing Signature DB===")

    fetchedDb.close()
    fetchingDb.close()

    println("[Signature DB CLOSED]\n")
  }

}
