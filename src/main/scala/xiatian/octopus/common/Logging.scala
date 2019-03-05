package xiatian.octopus.common

import org.slf4j.LoggerFactory

/**
  * 统一的日志接口
  */
trait Logging {
  val LOG = LoggerFactory.getLogger(this.getClass)

}

object Logging {
  def println(message: String): Unit = {
    System.out.println(message)
  }
}
