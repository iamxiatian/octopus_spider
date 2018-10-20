package xiatian.octopus.actor.fetcher

import akka.actor._
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import xiatian.octopus.actor.store.StoreActor
import xiatian.octopus.actor.{EmptyFetchJob, Fetch, FetchFailure, FetchJob, FetchRequest, FetchResult, LookupActor, NormalFetchJob}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}


/**
  * 一个爬虫客户端， fetcherId为唯一的代号
  *
  * @author Tian Xia
  *         Dec 03, 2016 23:28
  */
class FetchClientActor(remotePath: String, fetcherId: Int)
  extends LookupActor(remotePath) with ActorLogging {

  val store = context actorOf Props(new StoreActor())
  val crawler = context actorOf Props(new CrawlingActor(store))

  val NEW_REQUEST = FetchRequest(fetcherId)

  implicit val timeout = Timeout(10 seconds) //crawler的时间
  val requestNew = "request"
  val tick = context.system.scheduler.schedule(
    0 millis,
    15000 millis,
    self,
    requestNew)
  //如果长时间得不到服务器的返回相应，则自动发送NEW_REQUEST
  var lastReceivedTaskTime = System.currentTimeMillis()
  var emptyCount = 0

  override def active(master: ActorRef): Actor.Receive = {
    case op: FetchRequest => master ! op

    case `requestNew` =>
      //如果5分钟内没有接收到过任务，则自动向master发起任务请求
      if (System.currentTimeMillis() - lastReceivedTaskTime > 300000)
        master ! NEW_REQUEST

    case result: FetchJob =>
      lastReceivedTaskTime = System.currentTimeMillis() //更新同步时间
      result match {
        case EmptyFetchJob() =>
          print("🈳")
          emptyCount += 1
          val millis = 2000 + (emptyCount * 1000)
          //最长延迟2分钟
          val delay = if (millis > 120000) 12000 else millis
          Thread.sleep(delay)
          master ! NEW_REQUEST
        case NormalFetchJob(link, context, proxyHolder) =>
          log.debug(s"fetch: $link")
          print("\uD83D\uDE0A") // 取到正常任务的符号：😊
          emptyCount = 0
          //指定30秒的延迟, 设置较长的时间延迟，保证GC能够及时回收内存
          // 如果依然有问题，考虑在CrawlingActor中限制最长的运行时间
          (crawler ? Fetch(link, fetcherId, context, proxyHolder)) (60 seconds)
            .mapTo[FetchResult]
            .recoverWith {
              case e =>
                e.printStackTrace()
                log.error(s"Fetch ${link.url} error ==> $e")
                Future {
                  FetchFailure(link, e.toString, fetcherId)
                }
            }
            .pipeTo(master)
            .onComplete {
              case Success(r) =>
                log.info(s"Crawled and send back to master: ${link.url}")
              case Failure(e) =>
                e.printStackTrace()
                log.error(s"Error crawling $link", e)
            }
      }

    case Terminated(`master`) =>
      if (log.isWarningEnabled) log.warning("Master terminated")

      sendIdentifyRequest()
      context.become(identifying)
    case ReceiveTimeout =>
    // ignore
  }

  override def afterActive(master: ActorRef): Unit = {
    master ! NEW_REQUEST
    println(s"\nFetcher $fetcherId has connected to the master")
  }

}
