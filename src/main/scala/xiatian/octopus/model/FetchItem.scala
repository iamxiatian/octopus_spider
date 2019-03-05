package xiatian.octopus.model

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.net.URL

import xiatian.octopus.FastSerializable
import xiatian.octopus.util.{HashUtil, HexBytesUtil}

import scala.util.{Failure, Success, Try}


/**
  * 一个抓取条目，抓取条目可以是一个url，也可以是一个公众号，或者其他可以单独处理的条目
  *
  * @param value   描述抓取条目的值
  * @param depth   采集的深度，默认为0，其子链接的深度为1，以此类推
  * @param retries 连续抓取失败的次数
  * @param `type`  类型
  * @param taskId  : 表明该链接是由哪个栏目抓取得到的，如果为"auto"， 则通过自动抽取进行处理
  */
case class FetchItem(value: String,
                     `type`: FetchType,
                     refer: Option[String] = None,
                     anchor: Option[String] = None,
                     depth: Int = 0,
                     retries: Byte = 0, //最大为20,超过100则为20
                     taskId: String,
                     params: Map[String, String] = Map.empty[String, String]
                    ) {

  def hasParam(key: String): Boolean = params.contains(key)

  def getParam(key: String): Option[String] = params.get(key)

  def getParamOrElse(key: String, default: String): String = params.getOrElse(key, default)

  def toBytes(): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val dos = new DataOutputStream(out)

    dos.writeByte(FetchItem.VERSION)
    dos.writeUTF(value)

    dos.writeUTF(refer.getOrElse(""))
    dos.writeUTF(anchor.getOrElse(""))

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
    new URL(value).getHost
  } match {
    case Success(host) => host
    case Failure(e) =>
      e.printStackTrace()
      value
  }

  /**
    * 复制对象，并把retries变为新的数值
    */
  def copy(newRetries: Int): FetchItem = FetchItem(
    value,
    `type`,
    refer,
    anchor,
    depth,
    if (newRetries > 10) 10.toByte else newRetries.toByte,
    taskId,
    params
  )

  /**
    * URL哈希值的十六进制表示结果，例如：
    * 2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824
    *
    * @return
    */
  def urlHashHex: String = HexBytesUtil.bytes2hex(urlHash)

  def urlHash: Array[Byte] = HashUtil.hashAsBytes(url)

  def url = value
}

object FetchItem {
  val VERSION = 1


  def readFrom(bytes: Array[Byte]): FetchItem = {
    val din = new DataInputStream(new ByteArrayInputStream(bytes))

    val item = readFrom(din)
    din.close()

    item
  }

  def readFrom(din: DataInputStream): FetchItem = {
    val version = din.readByte() //版本

    val url = din.readUTF()

    val referText = din.readUTF()
    val refer = if (referText.isEmpty) None else Option(referText)

    val anchorText = din.readUTF()
    val anchor = if (anchorText.isEmpty) None else Option(anchorText)

    val depth = din.readInt()
    val retries = din.readByte()
    val typeId = din.readInt()
    val `type` = FetchType(typeId)
    //val updateFrequency = din.readLong()
    val taskId = din.readUTF()

    val size = din.readInt()
    val params = (1 to size).map {
      _ =>
        (din.readUTF(), din.readUTF())
    }.toMap

    FetchItem(url, `type`, refer, anchor, depth, retries, taskId, params)
  }
}
