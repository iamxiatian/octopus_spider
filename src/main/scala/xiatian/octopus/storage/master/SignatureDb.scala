package xiatian.octopus.storage.master

import java.io.File

import com.google.common.primitives.Longs
import org.rocksdb.{Options, RocksDB}
import org.zhinang.util.cache.LruCache
import xiatian.octopus.common.MyConf
import xiatian.octopus.storage.Db
import xiatian.octopus.util.HexBytesUtil

import scala.util.Try

/**
  * URL的签名库，通过签名记录已经采集过的特定
  *
  * @param name      签名数据库的名称
  * @param dbPath    数据库的保存路径
  * @param cacheSize 缓存大小
  */
private[master] class SignatureDb(name: String,
                                  dbPath: File,
                                  cacheSize: Int) extends Db {
  RocksDB.loadLibrary()

  if (!dbPath.exists()) {
    val created = dbPath.getParentFile.mkdirs()
    if (!created) {
      println(s"创建数据库目录失败！ ==> ${dbPath.getCanonicalPath}")
    }
  }

  private val options = new Options().setCreateIfMissing(true)
  private val db = RocksDB.open(options, dbPath.getCanonicalPath)
  /**
    * 数据库中有的数据的缓存
    */
  private val hitCache = new LruCache[String, Long](cacheSize)
  /**
    * 数据库中不存在数据的缓存，避免每次都进一步判断数据库是否有
    */
  private val notHitCache = new LruCache[String, Long](cacheSize)

  def open() = {
    println(s"open signature db $name at path ${dbPath.getCanonicalPath}" +
      s" (keys: ${numKeys()})")
  }

  def numKeys() = db.getLongProperty("rocksdb.estimate-num-keys")

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

  def remove(hash: Array[Byte]): Unit = {
    val hex = HexBytesUtil.bytes2hex(hash)
    hitCache.remove(hex)
    notHitCache.put(hex, System.currentTimeMillis())
    db.delete(hash)
  }

  def close() = {
    print(s"\tclose signature db $name at ${dbPath.getCanonicalPath}...")
    db.close()
    println("DONE")
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

/**
  * 已经抓取的链接数据库签名
  */
object FetchedSignatureDb extends SignatureDb(
  "Fetched Signature Database",
  new File(MyConf.masterDbPath, "fetched/signature"),
  MyConf.masterDbCacheSize) {

}

/**
  * 正在进行抓取的链接签名数据库
  */
object FetchingSignatureDb extends SignatureDb(
  "Fetched Signature Database",
  new File(MyConf.masterDbPath, "fetching/signature"),
  MyConf.masterDbCacheSize) {

}
