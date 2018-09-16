package xiatian.spider.model

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.net.URL

import xiatian.common.util.{HashUtil, HexBytesUtil}
import xiatian.spider.FastSerializable

import scala.util.{Failure, Success, Try}

/**
  * 爬虫的URL链接对象, 单独的URL包含的信息太少，所以采用FetchLink进行封装
  *
  * @author Tian Xia
  *         May 09, 2017 00:11
  */

/**
  * 爬虫的URL链接对象
  *
  * @param url
  * @param depth 采集的深度，默认为0，其子链接的深度为1，以此类推
  * @param retries 连续抓取失败的次数
  * @param `type`  类型
  * @param taskId  : 表明该链接是由哪个栏目抓取得到的，如果为"auto"， 则通过自动抽取进行处理
  */
case class FetchLink(url: String,
                     refer: Option[String] = None,
                     anchor: String = "",
                     depth: Int = 0,
                     retries: Byte = 0, //最大为20,超过100则为20
                     `type`: LinkType,
                     //refreshInSeconds: Long = 86400L * 365 * 1000, //更新周期, 以秒为单位, 默认为1000年，即不再更新
                     taskId: String,
                     params: Map[String, String] = Map.empty[String, String]
                    ) extends FastSerializable {

  def hasParam(key: String): Boolean = params.contains(key)

  def getParam(key: String): Option[String] = params.get(key)

  def getParamOrElse(key: String, default: String): String = params.getOrElse(key, default)

  def toBytes(): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val dos = new DataOutputStream(out)

    dos.writeByte(FetchLink.VERSION)
    dos.writeUTF(url)

    if (refer.isEmpty) {
      dos.writeBoolean(false)
    } else {
      dos.writeBoolean(true)
      dos.writeUTF(refer.get)
    }

    if (anchor == null) {
      dos.writeUTF("")
    } else {
      dos.writeUTF(anchor)
    }

    dos.writeInt(depth)
    dos.writeByte(retries)
    dos.writeInt(`type`.id)
    //dos.writeLong(refreshInSeconds)
    dos.writeUTF(taskId)

    dos.writeInt(params.size)
    params.foreach {
      case (k, v) =>
        dos.writeUTF(k)
        dos.writeUTF(v)
    }

    dos.close()
    out.close()

    out.toByteArray
  }

  def getHost(): String = Try {
    new URL(url).getHost
  } match {
    case Success(host) => host
    case Failure(e) =>
      e.printStackTrace()
      url
  }

  /**
    * 复制对象，并把retries变为新的数值
    */
  def copy(newRetries: Int): FetchLink = FetchLink(
    url,
    refer,
    anchor,
    depth,
    if (newRetries > 20) 20 else newRetries.toByte,
    `type`,
    //refreshInSeconds,
    taskId,
    params
  )

  def urlHash: Array[Byte] = HashUtil.hashAsBytes(url)

  /**
    * URL哈希值的十六进制表示结果，例如：
    * 2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824
    * @return
    */
  def urlHashHex: String = HexBytesUtil.bytes2hex(urlHash)
}

object FetchLink {
  val VERSION = 1

  def readFrom(bytes: Array[Byte]): FetchLink = {
    val din = new DataInputStream(new ByteArrayInputStream(bytes))

    val version = din.readByte() //当期那版本

    val url = din.readUTF()

    val hasRefer = din.readBoolean()
    val refer = if (hasRefer) Some(din.readUTF()) else None

    val anchor = din.readUTF()
    val depth = din.readInt()
    val retries = din.readByte()
    val typeId = din.readInt()
    val `type` = LinkType(typeId)
    //val updateFrequency = din.readLong()
    val taskId = din.readUTF()

    val size = din.readInt()
    val params = (1 to size).map {
      _ =>
        (din.readUTF(), din.readUTF())
    }.toMap

    din.close

    FetchLink(url, refer, anchor, depth, retries, `type`, taskId, params)
  }
}
