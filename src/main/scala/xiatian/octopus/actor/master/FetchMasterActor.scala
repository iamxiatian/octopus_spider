package xiatian.octopus.actor.master

import java.text.SimpleDateFormat

import akka.actor.SupervisorStrategy.{Restart, Resume}
import akka.actor.{Actor, ActorLogging, ActorRef, OneForOneStrategy}
import akka.remote.{AssociatedEvent, AssociationErrorEvent, AssociationEvent, DisassociatedEvent}
import xiatian.octopus.actor._
import xiatian.octopus.common.{Logging, MyConf}
import xiatian.octopus.model.Context
import xiatian.octopus.storage.master.{FetchLogDb, WaitDb}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._


/**
  * 爬虫的URL和状态控制中心。
  *
  * @author Tian Xia
  *         Dec 03, 2016 22:39
  */
class FetchMasterActor extends Actor with ActorLogging {

  import MasterVariable._

  log.warning("Start fetch master actor...")

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
      case e: ArithmeticException =>
        log.error(s"ArithmeticException in FetchMaster: $e")
        e.printStackTrace()
        Resume
      case e: NullPointerException =>
        log.error(s"NullPointerException in FetchMaster: $e")
        e.printStackTrace()
        //Restart
        Resume
      case e: IllegalArgumentException =>
        log.error(s"IllegalArgumentException in FetchMaster: $e")
        e.printStackTrace()
        //Stop
        Resume
      case e: Exception =>
        log.error(s"Exception in FetchMaster: $e")
        e.printStackTrace()
        Restart
      //Escalate
    }
  val startTime = System.currentTimeMillis()
  val dayFormat = new SimpleDateFormat("yyyMMdd HH:mm:ss")
  val startTimeMsg = dayFormat.format(startTime) //开始时间提示

  override def preStart: Unit = {
    context.system.eventStream.subscribe(self, classOf[AssociationEvent])
    context.system.eventStream.subscribe(self, classOf[AssociationErrorEvent])
  }

  override def postStop(): Unit = {
    log.warning("Shutdown FetchMaster...")
  }

  override def receive: Receive = {
    case FetchRequest(id) =>
      //Logging.println(s"task request $id from fetcher ${sender().path}")
      sendFetchTask(sender, id)

    case FetchResult(fetcherId, code, link, childLinks, anchorLinks, message) =>
      if (FetchCode.isOk(code)) {
        UrlManager.markFetched(link)
        UrlManager.removeFetching(link)

        UrlManager.countSuccess(link)

        //异步统计抓取链接信息
        Future(countFetchItem(link))

        //重新加入到等待抓取数据库
        Future.successful {
          WaitDb.push(if (link.retries > 0) link.copy(0) else link)
        }
        //把子链接入爬行队列
        childLinks.foreach {
          childLink =>
            UrlManager.pushLink(childLink, true)
        }
      } else {
        //记录错误信息
        log.error(s"Fetch ${link.url} is not success, code: ${code}")
        UrlManager.markDead(link)
        UrlManager.removeFetching(link)

        UrlManager.countFailure(link)

        //重新加入到等待抓取数据库
        Future.successful {
          if (link.retries > MyConf.maxLinkRetries) {
            log.warning(s"skip ${link.url}, beyond max retries.")
          } else WaitDb.push(link.copy(link.retries + 1))
        }

        log.warning(s"Finished fetch but found error ==> ${link}")
      }

      val address = sender.path.address
      val fromHost = address.host.getOrElse("") + ":" + address.port.getOrElse(0)
      FetchLogDb.log(fromHost, link.`type`.name, link.url, code, message.getOrElse(""))

      Logging.println(s"Fetched ${link.url} with http code, $code")
      sendFetchTask(sender(), fetcherId)

    case FetchFailure(link, reason, fetcherId) =>
      failureLinkCount += 1

      log.warning(s"Dead link: $link, $reason")
      UrlManager.markDead(link)
      UrlManager.countFailure(link)

      //重新加入到等待抓取数据库
      Future.successful {
        WaitDb.push(link.copy(link.retries + 1))
      }

      //日志记录到LogDb中
      val address = sender.path.address
      val fromHost = address.host.getOrElse("") + ":" + address.port.getOrElse(0)
      FetchLogDb.log(fromHost, link.`type`.name, link.url, 600, reason)

      sendFetchTask(sender(), fetcherId)


    case AssociatedEvent(localAddress, remoteAddress, inbound) =>
      log.warning(s"Master AssociatedEvent info : local address is" +
        s" $localAddress, remote address is $remoteAddress," +
        s"inbound is $inbound")
      remoteAddress.host.foreach(connectedFetchers.add)

    case DisassociatedEvent(localAddress, remoteAddress, inbound) =>
      log.warning(s"Master DisassociatedEvent info : local address is" +
        s" $localAddress, remote address is $remoteAddress," +
        s"inbound is $inbound")
      remoteAddress.host.foreach(connectedFetchers.remove)


    case e: AssociationErrorEvent =>
      log.info(s"AssociationErrorEvent: $e")
      if (e.cause.getCause.getMessage.contains("quarantined this system")) {
        log.warning(s"We got quarantined")
      }

    case FetchStatsRequest() =>
      sender() ! FetchStatsReply("TODO")
  }

  def sendFetchTask(currentSender: ActorRef, fetcherId: Int) = {
    //爬虫的主机地址
    val fetcherHost = currentSender.path.address.host.getOrElse("127.0.0.1")
    //Logging.println(s"send fetch task to: $fetcherHost")

    BucketController.getFetchItem(fetcherHost, fetcherId) match {
      case Some(link) =>
        val c = Context(version)

        //把当前链接标记为正在抓取，且不在桶中
        UrlManager.markFetching(link)

        val proxyHolder: Option[ProxyIp] = ProxyIpPool.take()

        //print("#")
        currentSender ! NormalFetchJob(link, c, proxyHolder)

        //        currentSender ! "HELLO"

        //log.info(s"send ${link.url} to fetcher ${fetcherId}")

        //把当前链接标记为正在抓取，且不在桶中
        BucketController.removeFromBucket(link)
      case None =>
        currentSender ! EmptyFetchJob()
    }
  }

}
