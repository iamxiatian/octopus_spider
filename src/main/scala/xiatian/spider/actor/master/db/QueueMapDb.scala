package xiatian.spider.actor.master.db

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger

import com.google.common.collect.Lists
import com.google.common.primitives.Ints
import org.rocksdb._

import scala.util.Try

/**
  * 存储键值对的循环队列数据库，里面记录了front和rear两个指针，循环使用空间
  * 空间大小通过capacity设置
  *
  * @param path
  * @param capacity 队列的容量
  */
abstract class QueueMapDb(path: String,
                          capacity: Int = 100000000
                         ) extends Db {
  RocksDB.loadLibrary()

  private val options = new DBOptions()
    .setCreateIfMissing(true)
    .setCreateMissingColumnFamilies(true)


  //默认的Column Family下标位置
  private val DEFAULT_CF_IDX = 0;

  //元数据（Front，Rear等信息）Column Family下标位置
  private val META_Q_CF_IDX = 1;

  private val cfNames = Lists.newArrayList[ColumnFamilyDescriptor](
    new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
    new ColumnFamilyDescriptor("metaQ".getBytes()) //队列元数据
  )

  private val cfHandlers = Lists.newArrayList[ColumnFamilyHandle]

  private val db = RocksDB.open(options, path, cfNames, cfHandlers)

  private val defaultHandler = cfHandlers.get(0)
  private val metaHandler = cfHandlers.get(1)

  override def open() = {
    println(s"Successfully open db $path")
  }

  private val FRONT_KEY = "#FRONT".getBytes(StandardCharsets.UTF_8)
  private val REAR_KEY = "#REAR".getBytes(StandardCharsets.UTF_8)

  // Next Emputy Position
  private val front = {
    val value = db.get(metaHandler, FRONT_KEY)
    if (value == null)
      new AtomicInteger(0)
    else
      new AtomicInteger(Ints.fromByteArray(value))
  }

  // Next Full Position
  private val rear = {
    val value = db.get(metaHandler, REAR_KEY)
    if (value == null)
      new AtomicInteger(0)
    else
      new AtomicInteger(Ints.fromByteArray(value))
  }

  /**
    * 入队
    */
  def enqueue(key: Array[Byte], value: Array[Byte]): Boolean = {
    if (full()) {
      println("Full!")
      false
    } else {
      synchronized {
        val idx = front.getAndSet((front.intValue() + 1) % capacity)
        db.put(metaHandler, FRONT_KEY, Ints.toByteArray(front.intValue()))
        db.put(metaHandler, Ints.toByteArray(idx), key)

        db.put(defaultHandler, key, value)

        true
      }
    }
  }

  def frontPosition() = front.get()

  def rearPosition() = rear.get()

  def count() =
    if (front.get >= rear.get)
      (front.get - rear.get)
    else
      capacity - (rear.get - front.get)

  /**
    * 是否已经在队列中了
    */
  def containsKey(key: Array[Byte]) = {
    db.get(defaultHandler, key) != null
  }

  /**
    * 出对列
    */
  def dequeue(): Option[Array[Byte]] = {
    if (empty()) None
    else {
      synchronized {
        val idx = rear.getAndSet((rear.intValue() + 1) % capacity)
        val key = db.get(metaHandler, Ints.toByteArray(idx))
        db.put(metaHandler, REAR_KEY, Ints.toByteArray(rear.intValue()))

        if (key == null) {
          None
        } else {
          db.delete(metaHandler, Ints.toByteArray(idx))
          val value = db.get(defaultHandler, key)
          if (value == null) {
            None
          } else {
            db.delete(key)
            Some(value)
          }
        }
      }
    }
  }

  /**
    * 首部出队列的元素，重新归还到首部
    */
  def returnQueue(key: Array[Byte], value: Array[Byte]): Boolean = {
    if (full()) {
      println("Full!")
      false
    } else {
      synchronized {
        val idx = if (rear.intValue() == 0) {
          capacity - 1
        } else {
          rear.intValue() - 1
        }

        rear.set(idx)

        db.put(metaHandler, Ints.toByteArray(idx), key)
        db.put(defaultHandler, key, value)
        db.put(metaHandler, REAR_KEY, Ints.toByteArray(rear.intValue()))

        true
      }
    }
  }

  def empty() = front.intValue() == rear.intValue()

  def full() = (front.intValue() + 1) % capacity == rear.intValue()

  def close() = {
    cfHandlers.forEach(_.close)
    if (db != null) db.close
    if (options != null) options.close
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

//object QueueDb {
//  def main(args: Array[String]): Unit = {
//    val queue = new QueueDb("/tmp/queuetest", 1000)
//    for (i <- 1 to 1000) {
//      queue.enqueue(Ints.toByteArray(i), s"hello ${i}".getBytes)
//    }
//    val first = queue.dequeue()
//    if (first.isEmpty) {
//      println("Empty")
//    } else {
//      println(new String(first.get))
//    }
//  }
//}