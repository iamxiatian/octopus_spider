package xiatian.octopus.actor.master

import java.util.concurrent.LinkedBlockingQueue

import xiatian.octopus.actor.ProxyIp

import scala.collection.JavaConverters._

/**
  * IP代理池, 运行在Master端，Master在通过FetchNormalTask向Fetcher发送抓取任务时，同时
  * 把当前需要使用的代理地址以FetchNormalTask的参数方式，发送给Fetcher，供Fetcher使用。
  *
  * @author Tian Xia
  *         School of IRM, Renmin University of China.
  *         Sep 24, 2017 09:27
  */
object ProxyIpPool {
  /** 维持当前有效的IP地址 */
  val ips = new LinkedBlockingQueue[ProxyIp]


  def addProxy(host: String, port: Int, expiredMillis: Long) =
    ips.add(ProxyIp(host, port, expiredMillis))

  def addProxy(proxyIp: ProxyIp) = ips.add(proxyIp)

  def clear = ips.clear()

  def take(): Option[ProxyIp] =
    if (ips.size() == 0) {
      None
    } else {
      val proxyIp = ips.take()
      if (proxyIp.expired()) {
        take()
      } else {
        ips.add(proxyIp)
        Some(proxyIp)
      }
    }

  def listProxyIps(): List[ProxyIp] = ips.iterator().asScala.toList
}
