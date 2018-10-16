package xiatian.octopus.util

import java.net.NetworkInterface

import scala.collection.JavaConverters._

/**
  * 主机相关的信息
  *
  * @author Tian Xia
  *         Jul 05, 2017 17:06
  */
object Machine {
  def getLocalIp(): Option[String] = {

    try {
      val interfaces = NetworkInterface.getNetworkInterfaces.asScala.toList

      interfaces.filterNot(
        networkInterface =>
          networkInterface.isLoopback()
            || networkInterface.isVirtual()
            || !networkInterface.isUp()
      ).flatMap {
        networkInterface =>
          networkInterface.getInetAddresses().asScala.toList.map(
            _.getHostAddress
          )
      }.headOption

    } catch {
      case e: Exception =>
        None
    }
  }
}