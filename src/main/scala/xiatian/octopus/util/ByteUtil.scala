package xiatian.octopus.util

import java.nio.ByteBuffer

/**
  * 字节处理的工具类，包括字节和十六进制字符串的转换，各种数字类型与字节数组的转换
  */
object ByteUtil {

  def short2bytes(n: Short): Array[Byte] = ByteBuffer.allocate(2).putShort(n).array()

  def int2bytes(n: Int): Array[Byte] = ByteBuffer.allocate(4).putInt(n).array()

  def long2bytes(n: Long): Array[Byte] = ByteBuffer.allocate(8).putLong(n).array()

  def float2bytes(n: Float): Array[Byte] = ByteBuffer.allocate(4).putFloat(n).array()

  def double2bytes(n: Double): Array[Byte] = ByteBuffer.allocate(8).putDouble(n).array()

  def bytes2short(bytes: Array[Byte]): Short = ByteBuffer.wrap(bytes).getShort

  def bytes2Int(bytes: Array[Byte]): Int = ByteBuffer.wrap(bytes).getInt

  def bytes2Long(bytes: Array[Byte]): Long = ByteBuffer.wrap(bytes).getLong

  def bytes2float(bytes: Array[Byte]): Float = ByteBuffer.wrap(bytes).getFloat

  def bytes2double(bytes: Array[Byte]): Double = ByteBuffer.wrap(bytes).getDouble

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