package xiatian.octopus.storage

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Arrays
import java.util.concurrent.atomic.AtomicLong

import com.google.common.collect.Lists
import com.google.common.primitives.Longs
import org.rocksdb._
import org.slf4j.LoggerFactory
import xiatian.octopus.storage.ScoreUpdate.{ALWAYS, GT_OLD, LT_OLD, NEVER}

import scala.util.Try

/**
  * 基于RocksDB实现的类似于Redis的SortedSet功能。在此数据库中，每个元素具有一个score和
  * 对应的value，并且可以按照score定位特定范围内的元素。
  *
  * 实现思路：
  * 对于一个(key, value, score)三元组：RocksDB中插入两组：
  * 第一个ColumnFamily（SCORE_SORT_COLUMN）: (score+key --> value)
  * 第二个ColumnFamily（KEY_SCORE_COLUMN）: (key --> score)
  *
  */
class SortedSetDb(dbName: String, //数据库的别名，方便调试和日志查看
                  path: String) extends Db {
  protected val LOG = LoggerFactory.getLogger(this.getClass)

  RocksDB.loadLibrary()

  private val options = new DBOptions()
    .setCreateIfMissing(true)
    .setCreateMissingColumnFamilies(true)

  //默认的Column Family下标位置
  private val SCORE_SORT_COLUMN = 0

  //元数据（Front，Rear等信息）Column Family下标位置
  private val KEY_SCORE_COLUMN = 1

  //存放元数据的列：如记录数量
  private val META_COLUMN = 2

  private val cfNames = Lists.newArrayList[ColumnFamilyDescriptor](
    new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
    new ColumnFamilyDescriptor("key_score".getBytes()), //队列元数据
    new ColumnFamilyDescriptor("meta".getBytes())
  )

  private val cfHandlers = Lists.newArrayList[ColumnFamilyHandle]

  //打开数据库，后面才可以获取到ColumnFamilyHandler
  private val db = RocksDB.open(options, path, cfNames, cfHandlers)

  private val scoreSortHandler = cfHandlers.get(0)
  private val keyScoreHandler = cfHandlers.get(1)
  private val metaHandler = cfHandlers.get(2)

  /**
    * 记录数据库中key的总数量
    */
  private val keyCounter: AtomicLong = {
    val old = db.get(metaHandler, "counter".getBytes(UTF_8))
    if (old == null) new AtomicLong(0)
    else {
      val n = Longs.fromByteArray(old)
      new AtomicLong(if (n >= 0) n else 0)
    }
  }

  /**
    * 更新主键对应的分值
    */
  def modifyScore(key: Array[Byte], score: Long): Boolean = {
    val scoreArray = Longs.toByteArray(score)
    //先查看原先的key是否已经在排序队列中了。
    val oldScoreArray: Array[Byte] = db.get(keyScoreHandler, key)
    if (oldScoreArray == null) {
      LOG.warn(s"key does not exist.")
      false
    } else if (Arrays.equals(scoreArray, oldScoreArray)) {
      true
    } else {
      //删除原先的score_sort列中的对应元素
      val oldScoreKey = oldScoreArray ++ key
      val value = db.get(oldScoreKey)

      db.delete(scoreSortHandler, oldScoreKey)

      if (value != null) {
        //先更新key对应的分值
        db.put(keyScoreHandler, key, scoreArray)

        //再将原先的value插入到新的score+key主键下
        db.put(scoreSortHandler, scoreArray ++ key, value)

        true
      } else false
    }
  }

  /**
    * 把(key, value, score)三元组保存到数据库中,当key在数据库中已经存在时，需要根据覆盖策略
    * 处理, 如果符合更新策略要求，则更新原有的score和对应的value内容，否则，只更新value内容。
    */
  def put(key: Array[Byte], value: Array[Byte], score: Long, strategy: ScoreUpdate): Unit = {
    val scoreArray = Longs.toByteArray(score)

    //先查看原先的key是否已经在排序队列中了。
    val oldScoreArray: Array[Byte] = db.get(keyScoreHandler, key)
    if (oldScoreArray != null) {
      val oldScore = Longs.fromByteArray(oldScoreArray)

      val replace = strategy match {
        case ALWAYS => true
        case NEVER => false
        case LT_OLD => score < oldScore
        case GT_OLD => score > oldScore
      }

      if (replace) {
        //删除原先的score_sort列中的对应元素
        val scoreSortKey = oldScoreArray ++ key
        db.delete(scoreSortHandler, scoreSortKey)

        //插入新记录, 先插入keyScore列
        db.put(keyScoreHandler, key, scoreArray)

        //再插入scoreSort列, 方便按照scoreSort中元素的大小顺序进行访问
        db.put(scoreSortHandler, scoreArray ++ key, value)
      } else {
        //只更新key对应的内容，不更新score
        db.put(scoreSortHandler, oldScoreArray ++ key, value)
      }

    } else {
      val count = keyCounter.incrementAndGet()
      if (count % 1000 == 0) {
        saveKeyCounter()
      }

      //插入新记录, 先插入keyScore列
      db.put(keyScoreHandler, key, scoreArray)

      //再插入scoreSort列, 方便按照scoreSort中元素的大小顺序进行访问
      db.put(scoreSortHandler, scoreArray ++ key, value)
    }
  }

  /**
    * 返回该key对应的内容和score二元组
    *
    * @param key
    * @return
    */
  def get(key: Array[Byte]): Option[(Array[Byte], Long)] = {
    val scoreArray: Array[Byte] = db.get(keyScoreHandler, key)

    if (scoreArray != null) {
      val value = db.get(scoreSortHandler, scoreArray ++ key)
      if (value != null)
        Some((value, Longs.fromByteArray(scoreArray)))
      else
        None
    } else None
  }

  /**
    * 从数据库中弹出topN条符合条件的数据
    *
    * @param maxScore
    * @param topN
    * @return
    */
  def popTopList(maxScore: Long, topN: Int): Seq[(Array[Byte], Array[Byte], Long)] =
    elements(maxScore, topN, true)

  def findTopList(maxScore: Long, topN: Int): Seq[(Array[Byte], Array[Byte], Long)] =
    elements(maxScore, topN, false)

  /**
    * 得到小于等于maxScore的topN条记录，返回一个三元组序列（key, value, score）
    *
    * @param maxScore
    * @param topN
    * @param rm 是否同时删除读到的元素，当pop时，会启用该参数
    * @return
    */
  private def elements(maxScore: Long, topN: Int, rm: Boolean): Seq[(Array[Byte], Array[Byte], Long)] = {
    val it = db.newIterator(scoreSortHandler)
    it.seekToFirst()

    val result = for (_ <- 1 to topN;
                      if (it.isValid &&
                        Longs.fromByteArray(it.key.splitAt(8)._1) <= maxScore)
    ) yield {
      //key的前面部分是Long型的score(前8个字节)，后面部分是key
      val (score, key) = it.key.splitAt(8)
      //println(Longs.fromByteArray(score))
      val value = it.value()

      //切换到下一条记录上
      it.next()

      (key, value, Longs.fromByteArray(score))
    }

    it.close()

    if (rm) result.foreach { case (key, _, _) => remove(key) }

    result
  }

  /**
    * 获取指定页码内的记录, 返回一个三元组序列（key, value, score）
    *
    * @return
    */
  def pageList(page: Int, pageSize: Int): Seq[(Array[Byte], Array[Byte], Long)] = {
    val start = (page - 1) * pageSize
    val end = page * pageSize

    val it = db.newIterator(scoreSortHandler)
    it.seekToFirst()

    for (_ <- 0 until start) {
      if (it.isValid) it.next()
    }

    val result = for (_ <- start until end;
                      if (it.isValid)) yield {
      //key的前面部分是Long型的score(前8个字节)，后面部分是key
      val (score, key) = it.key.splitAt(8)
      //println(Longs.fromByteArray(score))
      val value = it.value()

      //切换到下一条记录上
      it.next()

      (key, value, Longs.fromByteArray(score))
    }

    it.close()

    result
  }

  /**
    * 返回数据库中key的总数量
    *
    * @return
    */
  def count(): Long = keyCounter.longValue()

  override def clear: Try[Unit] = Try {
    val it = db.newIterator(keyScoreHandler)
    it.seekToFirst()
    while (it.isValid) {
      remove(it.key())
      it.next()
    }

    it.close()
    db.compactRange()
  }

  def remove(key: Array[Byte]): Unit = {
    //先查看原先的key是否已经在排序队列中了。
    val scoreArray: Array[Byte] = db.get(keyScoreHandler, key)
    if (scoreArray != null) {
      //删除原先的score_sort列中的对应元素
      val scoreSortKey = scoreArray ++ key
      db.delete(scoreSortHandler, scoreSortKey)

      keyCounter.decrementAndGet()
    }

    // 删除keyScore列
    db.delete(keyScoreHandler, key)
  }

  def open() = {

  }

  def close() = {
    println(s"===Close $dbName === \n\t $path ...")
    println("\t save counter...")
    saveKeyCounter()

    keyScoreHandler.close()
    scoreSortHandler.close()
    db.close()
    options.close()
    println(s"\t [$dbName CLOSED]\n")
  }

  /**
    * 把key的数量保存到数据库中
    */
  private def saveKeyCounter() = db.put(metaHandler,
    "counter".getBytes(UTF_8),
    Longs.toByteArray(keyCounter.longValue())
  )

  /**
    * 修复数据
    */
  override def repair() = {
    println("尝试修复计数器")
    val it = db.newIterator(keyScoreHandler)
    it.seekToFirst()
    var n = 0L
    while (it.isValid) {
      n = n + 1
      it.next()
    }
    it.close()

    keyCounter.set(n)
    saveKeyCounter()
  }
}

/**
  * score更新策略
  */
sealed trait ScoreUpdate

object ScoreUpdate {

  case object ALWAYS extends ScoreUpdate

  case object NEVER extends ScoreUpdate

  case object LT_OLD extends ScoreUpdate

  case object GT_OLD extends ScoreUpdate

}
