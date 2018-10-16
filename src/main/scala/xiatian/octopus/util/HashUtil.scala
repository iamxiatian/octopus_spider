package xiatian.octopus.util

import java.nio.charset.StandardCharsets

import com.google.common.base.Charsets
import com.google.common.hash.Hashing

/**
  * 采用Murmur 3算法实现的哈希处理，由于Murmur在64位和32位的计算机上，对相同内容输出的结果并
  * 不相同（因为优化原因造成），所以，请确保运行的计算机都采用64为计算机。
  * <br/>
  *
  * SEE: <a href="https://en.wikipedia.org/wiki/MurmurHash">Murmur algorithm</a>
  *
  * @author Tian Xia Email: xiat@ruc.edu.cn
  *         School of IRM, Renmin University of China.
  *         Jul 24, 2017 18:02
  */
object HashUtil {
  /**
    * 对一段字节数组生成一个唯一的长整数，作为其摘要结果
    *
    */
  def hashAsLong(content: Array[Byte]): Long =
    Hashing.sha256().hashBytes(content).asLong().abs

  //Hashing.murmur3_128(0).hashBytes(content).asLong()

  def hashAsBytes(content: Array[Byte]): Array[Byte] =
    Hashing.sha256().hashBytes(content).asBytes()

  //Hashing.murmur3_128(0).hashBytes(content).asBytes()

  /**
    * 对一段文字按照其utf8格式，转换为字节数组并计算哈希值
    */
  def hashAsLong(content: String): Long =
    Hashing.sha256().hashString(content, StandardCharsets.UTF_8).asLong().abs

  def hashAsBytes(content: String): Array[Byte] =
    Hashing.sha256().hashString(content, StandardCharsets.UTF_8).asBytes()


  def md5(content: String): String = Hashing.md5().newHasher()
    .putString(content, Charsets.UTF_8)
    .hash().toString

}
