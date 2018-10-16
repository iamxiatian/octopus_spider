package xiatian.octopus.actor.master.db

import java.io._

import org.joda.time.DateTime
import xiatian.octopus.common.MyConf

import scala.concurrent.Future

/**
  * 日志队列库
  */
class FetchLogDb(path: String, capacity: Int)
  extends QueueListDb(path, capacity) {
  def log(fetcherId: String, linkType: String, url: String, code: Int, message: String = "") =
    Future.successful(push(FetchLog(fetcherId, linkType, url, code).toBytes))

  def listLogs(page: Int, size: Int): List[FetchLog] = list(page, size).map(readFrom)

  private def readFrom(bytes: Array[Byte]): FetchLog = {
    val din = new DataInputStream(new ByteArrayInputStream(bytes))

    val fetcherId = din.readUTF()
    val linkType = din.readUTF()
    val url = din.readUTF()
    val code = din.readInt()
    val message = din.readUTF()
    val millis = din.readLong()
    FetchLog(fetcherId, linkType, url, code, message, new DateTime(millis))
  }

  override def close(): Unit = {
    super.close()
    println("LogDB closed.")
  }
}

case class FetchLog(fetcherId: String,
                    linkType: String,
                    url: String,
                    code: Int,
                    message: String = "",
                    fetchTime: DateTime = DateTime.now()) {
  def toBytes = {
    val out = new ByteArrayOutputStream()
    val dos = new DataOutputStream(out)

    dos.writeUTF(fetcherId)
    dos.writeUTF(linkType)
    dos.writeUTF(url)
    dos.writeInt(code)
    dos.writeUTF(message)
    dos.writeLong(fetchTime.getMillis)

    dos.close()
    out.close()

    out.toByteArray
  }
}

object FetchLogDb extends FetchLogDb(new File(MyConf.masterDbPath, "log").getCanonicalPath, 1000000)

