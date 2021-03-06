package xiatian.octopus.actor.master

import akka.actor.{Actor, ActorSystem}
import org.joda.time.DateTime
import xiatian.octopus.actor.ActorMessage.Starting
import xiatian.octopus.actor.ActorWatching
import xiatian.octopus.common.Logging
import xiatian.octopus.model.FetchItem
import xiatian.octopus.storage.master.{TaskDb, WaitDb}
import xiatian.octopus.task.FetchTask

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * 负责把全局URL队列插入到爬虫任务桶中，以及把XML采集任务注入到全局爬虫队列中
  *
  * @author Tian Xia
  *         Dec 05, 2016 13:41
  */
class InjectActor(system: ActorSystem) extends Actor with ActorWatching {
  val EACH_INJECT_COUNT = 5100 // 每次注入的任务数量

  var paused = false //是否暂停注入，默认为false，表示持续注入

  def receive = {
    case Starting =>
      self ! FillBucket
      self ! TransformJob
      self ! InjectTask

    case FillBucket => fillBucket()

    case InjectTask =>
      println(s"inject tasks at ${new DateTime().toString("HH:mm:ss")}")
      Try {
        injectTask()
      } match {
        case Success(_) =>
          system.scheduler.scheduleOnce(30 seconds, self, InjectTask)
        case Failure(e) =>
          e.printStackTrace()
          LOG.error(e.getMessage)
          system.scheduler.scheduleOnce(60 seconds, self, InjectTask)
      }

    case link: FetchItem =>
      LOG.info(s"Inject ${link.url}")
      UrlManager.pushLink(link, true)
  }

  /**
    * 把WaitDb中已经达到抓取日期的链接注入到系统中
    *
    * @return
    */
  def injectFromWaitDb() = Future {
    WaitDb.popCrawlLinks(1000).foreach {
      link =>
        UrlManager.pushLink(link, true)
    }
  }

  def injectTask() = {
    TaskDb.getIds().foreach {
      taskId =>
        Logging.println(s"inject $taskId")
        val task = FetchTask.get(taskId)
        if (task.nonEmpty) {
          val items = task.get.entryItems
          items.foreach {
            item =>
              UrlManager.pushLink(item, true)
          }
        }
    }
  }

  def fillBucket() = {
    if (paused) {
      LOG.debug("skip fill buckets due to PAUSE status")
      system.scheduler.scheduleOnce(29 seconds, self, FillBucket)
    } else {
      BucketController.fillBuckets().onComplete {
        case Success(counts) =>
          val totalFilled = counts.map {
            case (name, count) =>
              if (count > 0) {
                LOG.info(s"Filled $count $name to buckets.")
              }
              count
          }.sum

          //如果注入的链接数量较少，则下次尝试注入的时间加快
          val t = if (totalFilled < 200) 5 else 29
          system.scheduler.scheduleOnce(t seconds, self, FillBucket)
        case Failure(e) =>
          e.printStackTrace()
          LOG.error(s"Fill bucket error in Injector with exception: $e!")
          system.scheduler.scheduleOnce(29 seconds, self, FillBucket)
        case _ =>
          println("Fill bucket error in Injector!")
          system.scheduler.scheduleOnce(29 seconds, self, FillBucket)
      }
    }
  }


  sealed trait InjectAction

  case object FillBucket extends InjectAction

  case object TransformJob extends InjectAction

  case object InjectTask extends InjectAction

}
