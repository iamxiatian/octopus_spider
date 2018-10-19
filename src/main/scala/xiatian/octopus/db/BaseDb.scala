package xiatian.octopus.db

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8

import com.google.common.collect.Lists
import org.rocksdb.{ColumnFamilyDescriptor, ColumnFamilyHandle, DBOptions, RocksDB}
import org.slf4j.LoggerFactory
import xiatian.octopus.util.ByteUtil

import scala.util.{Failure, Success, Try}

/**
  * 带有两个列族的数据库基类
  *
  * @param dbName 用于显示使用的数据库的名称
  * @param dbPath 数据库的存储位置
  */
class BaseDb(val dbName: String, val dbPath: File) extends Db {
  protected val LOG = LoggerFactory.getLogger(this.getClass)

  protected val options = new DBOptions()
    .setCreateIfMissing(true)
    .setCreateMissingColumnFamilies(true)

  protected val cfNames = Lists.newArrayList[ColumnFamilyDescriptor](
    new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
    new ColumnFamilyDescriptor("meta".getBytes()) //元数据族
  )

  protected val cfHandlers = Lists.newArrayList[ColumnFamilyHandle]

  if (!dbPath.getParentFile.exists())
    dbPath.getParentFile.mkdirs()

  protected val db = RocksDB.open(options, dbPath.getAbsolutePath, cfNames, cfHandlers)

  protected val defaultHandler: ColumnFamilyHandle = cfHandlers.get(0)
  protected val metaHandler: ColumnFamilyHandle = cfHandlers.get(1)

  def numKeys(): Long = db.getLongProperty("rocksdb.estimate-num-keys")

  def open(): Unit = {
    println(s"open DB $dbName with count: ${numKeys()}")
  }

  def close(): Unit = {
    println(s"===Close DB $dbName === \n\t $dbPath ...")

    cfHandlers.forEach(_.close())
    if (db != null) db.close()
    if (options != null) options.close()

    println(s"  [DB $dbName CLOSED]\n")
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

  private def set(handler: ColumnFamilyHandle,
                  key: String,
                  value: Array[Byte]): this.type = Try {
    db.put(handler, key.getBytes(UTF_8), value)
  } match {
    case Success(_) => this
    case Failure(e) =>
      e.printStackTrace()
      LOG.error(s"error when save $key", e)
      this
  }

  /**
    * 类似于Redis的Hash类型，保存数据
    *
    * @return
    */
  private def hset(id: String,
                   handler: ColumnFamilyHandle,
                   attrName: String,
                   attrValue: Array[Byte]): this.type = Try {
    //采用id:name = value的形式保存
    val key = s"$id:$attrName".getBytes(UTF_8)

    db.put(handler, key, attrValue)
  } match {
    case Success(_) => this
    case Failure(e) =>
      e.printStackTrace()
      LOG.error(s"error when save $id (attrName=$attrName)", e)
      this
  }

  /**
    * 保存数据到默认的Column Family中
    */
  def hset(id: String, attrName: String, attrValue: Array[Byte]): this.type =
    hset(id, defaultHandler, attrName, attrValue)

  /**
    * 保存数据到Meta Column Family中
    */
  def hsetMeta(id: String, attrName: String, attrValue: Array[Byte]): this.type =
    hset(id, metaHandler, attrName, attrValue)


  def saveMetaString(key: String, value: String): this.type =
    set(metaHandler, key, value.getBytes(UTF_8))

  /**
    * 获取
    *
    * @param id
    * @param attrName
    * @return
    */
  def hget(id: String, attrName: String): Option[Array[Byte]] = Try {
    val key = s"$id:$attrName".getBytes(UTF_8)
    val value = db.get(defaultHandler, key)
    Option(value)
  } match {
    case Success(v) => v
    case Failure(e) =>
      LOG.error("hget error", e)
      e.printStackTrace()
      None
  }

  def hgetString(id: String, attrName: String): Option[String] =
    hget(id, attrName).map(new String(_, UTF_8))

  def hgetInt(id: String, attrName: String): Option[Int] =
    hget(id, attrName).map(ByteUtil.bytes2Int(_))

  def hgetDouble(id: String, attrName: String): Option[Double] =
    hget(id, attrName).map(ByteUtil.bytes2double(_))

  def hdel(id: String, attrName: String): Try[Unit] = Try {
    val key = s"$id:$attrName".getBytes(UTF_8)
    db.delete(defaultHandler, key)
  }

  def hdel(id: String) = Try {
    val key = id.getBytes(UTF_8)
    val it = db.newIterator(defaultHandler)
    it.seek(key)

    while (it.isValid && it.key().startsWith(key)) {
      db.delete(defaultHandler, it.key())
      it.next()
    }

    it.close()
  }

  /**
    * save string value to hash data structure.
    *
    * @param id
    * @param attrName
    * @param attrValue
    * @return
    */
  def hsave(id: String, attrName: String, attrValue: String): this.type =
    hset(id, attrName, attrValue.getBytes(UTF_8))

  def hsaveInt(id: String, attrName: String, attrValue: Int): this.type =
    hset(id, attrName, ByteUtil.int2bytes(attrValue))

  def hsaveDouble(id: String, attrName: String, attrValue: Double): this.type =
    hset(id, attrName, ByteUtil.double2bytes(attrValue))

  /**
    * save string value to hash data structure if value is not empty.
    *
    * @param id
    * @param attrName
    * @param attrValue
    * @return
    */
  def hsaveIfNotEmpty(id: String, attrName: String, attrValue: String): this.type =
    if (attrValue == null || attrValue.isEmpty)
      this
    else
      hsave(id, attrName, attrValue)
}
