package xiatian.octopus.util

import scala.util.Try

/**
  * ISBN Toolkit
  * 1. 10位到13位
  *
  * 10位的isbn编号是7111165616，转化到13位，遵循下面的算法：
  * 去掉最后一位，在最前面加上978，变成978711116561
  * 从第一个数字起，求每奇数位的和，记为a。9+8+1+1+6+6 = 31
  * 从第二个数字起，求每偶数位的和，记为b。7+7+1+1+5+1 = 22
  * 求a+3b，记为c。c = 97 求10-c并对结果取10的模，
  * 10 - c %1 0 = 3。这就是校验位，加在第一步结果的最后。
  * 得到13位编码为：9787111165613
  *
  *
  *
  *
  *
  * 2.从13位到10位的计算方法更简单 13位编号是9787111165613现在想把它转化位10位，
  * 可以这样做： 去掉开头的”978″和最后一位校验码，变成711116561 从第一位开始，
  * 将每一位和10到2的数字相乘，并求和；
  * 7*10 + 1*9 + 1* 8 + 1*7 + 1*6 + 6*5 + 5*4 + 6*3 + 1*2 = 170；记为c
  * 求11-c并对结果取11的模：(11-c)%11 = 6；如果结果是10就记为X，然后把算出来的一位
  * 加到第一步结果的最后，得到10位编码为：7111165616
  *
  */
object IsbnUtil {
  /**
    * 把isbn10转换为isbn13, 例如 0521865719 <->  9780521865715
    *
    * @param isbn10
    * @return
    */
  def to13(isbn10: String): Try[String] = Try {
    if (isbn10.length != 10)
      throw new Exception("ISBN10的位数不是10位数字")

    val code = "978" + isbn10.substring(0, isbn10.length - 1)

    val a = code.zipWithIndex
      .map { case (c, idx) => if (idx % 2 == 0) c - '0' else 0 }
      .sum

    val b = code.zipWithIndex
      .map { case (c, idx) => if (idx % 2 == 1) c - '0' else 0 }
      .sum

    val c = a + 3 * b

    val d = 10 - c % 10

    s"$code$d"
  }

  /**
    * 把isbn13转换为isbn10
    *
    * @param isbn13
    * @return
    */
  def to10(isbn13: String): Try[String] = Try {
    if (isbn13.length != 13)
      throw new Exception("ISBN13的位数不是13位数字")

    //去掉开头的978和最后一位校验码, 假设剩余为：711116561
    val code = isbn13.substring(3, 12)

    //从第一位开始，将每一位和10到2的数字相乘，并求和；7*10 + 1*9 + 1* 8 + 1*7 +
    //  1*6 + 6*5 + 5*4 + 6*3 + 1*2 = 170；记为c

    val c = code.zipWithIndex.map {
      case (c, idx) => (c - '0') * (10 - idx)
    }.sum

    val d = 11 - c % 11

    if (d == 11) s"${code}X" else s"$code$d"
  }

  /**
    * 把ISBN中的分隔符去除掉
    *
    * @param isbn
    * @return
    */
  def compact(isbn: String): String =
    isbn.filter(c => c >= '0' && c <= '9').trim

  /**
    * 该字符串是否是ISBN的一个子串
    * @param s
    * @return
    */
  def isPartOfIsbn(s: String): Boolean = {
    s.length >= 3 && s.forall(c => (c >= '0' && c <= '9') || c == '-')
  }

  def pretty(isbn: String): String = if (isbn.length == 10) {
    s"${isbn(0)}-${isbn.substring(1, 4)}-${isbn.substring(4, 9)}-${isbn(9)}"
  } else if (isbn.length == 13) {
    s"${isbn.substring(0, 3)}-${isbn(3)}-${isbn.substring(4, 8)}-${isbn.substring(8, 12)}-${isbn(12)}"
  } else isbn
}
