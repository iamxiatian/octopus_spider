package xiatian.octopus.db

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger

import com.google.common.primitives.Ints
import org.rocksdb._

import scala.util.Try

/**
  * 具有容量限制的存放元素的队列数据库. 数据库中元素的逻辑关系：
  *
  * rear: 1, 2, 3, ..., 100, 101, ..., n :head,
  *
  * 每次增加元素，head会加1, 达到容量后，会覆盖掉rear，同时rear前移。
  * 不同于QueueMapDb, QueueListDb数据库中的元素可以重复。
  *
  * QueueListDb 是基于 RocksDB 作为底层存储结构的,循环使用的队列,该队列拥
  * 有一个最大容量参数 (capacity),并维持了两个指针: front:指向下一个可放入元素的
  * 位置, rear:指向最开始放入的元素位置。初始时,front 和 rear 位于位置 0, 之后,每
  * 放入一个元素,front 指加 1, 当 front+1 = rear 时,则认为队列已满,需要将 rear 前
  * 移一个位置,再放入新元素,即等同于将对尾元素予以覆盖。
  *
  * @param path
  * @param capacity 队列的容量
  */
class QueueListDb(path: String,
                  capacity: Int = 100000000
                 ) extends Db {
  RocksDB.loadLibrary()

  private val options = new Options().setCreateIfMissing(true)
  private val db = RocksDB.open(options, path)

  def open() =
    println(s"Successfully open db $path")

  //FRONT_KEY表示下一个可以放入元素的位置
  private val FRONT_KEY = "#FRONT".getBytes(StandardCharsets.UTF_8)

  //REAR_KEY表示下一个可以读取的元素的位置
  private val REAR_KEY = "#REAR".getBytes(StandardCharsets.UTF_8)

  // Next Emputy Position
  private val front = {
    val value = db.get(FRONT_KEY)
    if (value == null)
      new AtomicInteger(0)
    else
      new AtomicInteger(Ints.fromByteArray(value))
  }

  // Next Full Position
  private val rear = {
    val value = db.get(REAR_KEY)
    if (value == null)
      new AtomicInteger(0)
    else
      new AtomicInteger(Ints.fromByteArray(value))
  }

  private def get(idx: Int): Array[Byte] = db.get(Ints.toByteArray(idx))

  /**
    * 增加元素
    */
  def push(e: Array[Byte]): Unit = {
    synchronized {

      if (full()) {
        rear.set((rear.intValue() + 1) % capacity)
        //rear 要前移一个，腾出空间以放入新的元素
        db.put(REAR_KEY, Ints.toByteArray(rear.get()))
      }

      val idx = front.getAndSet((front.intValue() + 1) % capacity)
      //更新FRONT_KEY的取值
      db.put(FRONT_KEY, Ints.toByteArray(front.intValue()))

      //在idx位置记录元素
      db.put(Ints.toByteArray(idx), e)
    }
  }

  /**
    * 弹出左侧的元素（即最早压入的元素）
    *
    * @return
    */
  def popLeft(): Option[Array[Byte]] = {
    if (!empty()) {
      val idx = rear.get()
      val e = db.get(Ints.toByteArray(idx))

      rear.set((rear.intValue() + 1) % capacity)
      //rear 要前移一个，腾出空间以放入新的元素
      db.put(REAR_KEY, Ints.toByteArray(rear.get()))

      Some(e)
    } else Option.empty[Array[Byte]]
  }

  /**
    * 获取指定页码和页码尺寸下，对应的元素列表，按照元素插入时间倒排
    *
    * @param page
    * @param size
    * @return
    */
  def list(page: Int = 1, size: Int = 100): List[Array[Byte]] = {
    val f = front.get()
    val r = rear.get()
    val skipCount = (page - 1) * size

    if (skipCount >= count())
      List.empty[Array[Byte]]
    else if (f > r) {
      val frontPosExclude = f - skipCount
      val rearPosInclude = Math.max(frontPosExclude - size, r)
      (rearPosInclude until frontPosExclude).toList.map {
        idx => get(idx)
      }.reverse
    } else {
      val frontPosExclude = if (f - skipCount >= 0)
        f - skipCount
      else
        capacity + (f - skipCount)

      (1 to size).toList.map {
        n =>
          //变换为要获取元素的位置
          val position = frontPosExclude - n
          if (position < 0) capacity + position else position
      }.takeWhile(_ + 1 != r) // 遇到rear之后的元素则不再获取
        .reverse
        .map(get)
    }
  }

  def frontPosition() = front.get()

  def rearPosition() = rear.get()

  def count() =
    if (front.get >= rear.get)
      (front.get - rear.get)
    else
      capacity - (rear.get - front.get)

  def empty() = front.intValue() == rear.intValue()

  def full() = (front.intValue() + 1) % capacity == rear.intValue()

  def close() = {
    if (db != null) db.close
    if (options != null) options.close
  }

  override def clear: Try[Unit] = Try {
    while (!empty()) popLeft()

    db.compactRange()
  }
}
