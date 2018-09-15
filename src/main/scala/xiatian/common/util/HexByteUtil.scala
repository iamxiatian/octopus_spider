package xiatian.common.util

object HexBytesUtil {

  def hex2bytes(hex: String): Array[Byte] =
    hex.replaceAll("[^0-9A-Fa-f]", "")
      .sliding(2, 2)
      .toArray
      .map(Integer.parseInt(_, 16).toByte)

  def bytes2hex(bytes: Array[Byte], sep: Option[String] = None): String =
    sep match {
      case None => bytes.map("%02x".format(_)).mkString
      case Some(s) => bytes.map("%02x".format(_)).mkString(s)
    }

  def example {
    val data = "48 65 6C 6C 6F 20 57 6F 72 6C 64 21 21"
    val bytes = hex2bytes(data)
    println(bytes2hex(bytes, Option(" ")))

    val data2 = "48-65-6C-6C-6F-20-57-6F-72-6C-64-21-21"
    val bytes2 = hex2bytes(data2)
    println(bytes2hex(bytes2, Option("-")))

    val data3 = "48656C6C6F20576F726C642121"
    val bytes3 = hex2bytes(data3)
    println(bytes2hex(bytes3))
  }

}
