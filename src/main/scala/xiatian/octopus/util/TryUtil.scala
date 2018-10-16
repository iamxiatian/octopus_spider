package xiatian.octopus.util

import scala.util.{Failure, Success, Try}

object TryUtil {
  /**
    * 对于一组Try结果，把有错误的信息输出出来, 没一行一个错误信息
    */
  def checkErrors(results: Seq[Try[_]]): Seq[String] =
    results map { //记录失败的字符串，如果成功，则返回"", 并在下一步过滤掉
      case Success(_) => ""
      case Failure(e) =>
        e.getMessage
    } filter (_.nonEmpty)
}
