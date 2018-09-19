package xiatian.spider.actor.master

import java.util.Calendar

import akka.actor.{Actor, ActorLogging, ActorSystem, Cancellable}
import org.joda.time.DateTime.now
import xiatian.common.MyConf
import xiatian.spider.actor.ActorMessage.Starting
import xiatian.spider.actor.ActorWatching

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._


/**
  * Master节点上执行的调度程序，目前的调度任务包括：
  *
  * <ul>
  * <li></li>
  * </ul>
  *
  * @author Tian Xia
  *         May 15, 2017 22:09
  */
class ScheduleActor(system: ActorSystem) extends Actor with ActorWatching {
  val triggerTimes = MyConf.mailTriggerTimes

  import ScheduleActor._

  def scheduling(): Cancellable = {
    nextScheduleJobs match {
      case Some((interval, jobs)) =>
        //jobs.foreach(self ! _)

        jobs.foreach {
          job =>
            system.scheduler.scheduleOnce(interval seconds, self, job)
        }

        //比计算出的延迟时间再延迟60秒, 进行下一次调度检查
        system.scheduler.scheduleOnce((interval + 60) seconds, self, Scheduling)
      case None =>
        log.warning("SCHEDULER TRIGGER TIMES is not SET" +
          "(scheduler.triggerTimes).")
        system.scheduler.scheduleOnce(10 minutes, self, Scheduling)
    }
  }

  def receive: Receive = {
    case Starting =>
      scheduling()

    case MailJob =>
      log.info(s"RUN JOB at ${now.toString("yyyy-MM-dd HH:mm:ss")}")
      sendMail()

    case Scheduling =>
      log.info(s"Scheduling jobs at ${now.toString("MM-dd HH:mm:ss")}")
      scheduling()
  }

  def sendMail() = {
    //if (MyConf.mailNotify)
    println(s"send mail now ===> ${new java.util.Date()}")
  }
}


object ScheduleActor {

  /**
    * 触发任务
    */
  case class Trigger(hour: Int, minute: Int, jobs: List[ScheduleJob]) {
    def toSeconds(): Long = {
      val c = Calendar.getInstance
      c.set(Calendar.HOUR_OF_DAY, hour)
      c.set(Calendar.MINUTE, minute)
      c.set(Calendar.SECOND, 0)
      c.set(Calendar.MILLISECOND, 0)
      c.getTimeInMillis / 1000
    }
  }

  case object Scheduling

  trait ScheduleJob

  case object MailJob extends ScheduleJob

  val triggers: List[Trigger] = {
    MyConf.mailTriggerTimes.map {
      case (hour, minute) => Trigger(hour, minute, List(MailJob))
    }
  }


  /**
    * 根据triggerTimes中提供的触发时间点列表，找出最近一次应该触发调度处理的
    * 时间点和对应的调度作业列表，并计算和当前时间的间隔，作为任务延迟调度的时间量
    *
    * triggerTimes like: List("8:00", "10:00", "12:00", "14:00", "17:00")
    *
    * @return 距离下次被调度的秒数，如没有满足条件的结果，则返回None
    */
  def nextScheduleJobs: Option[(Long, List[ScheduleJob])] = {
    val secondNow = System.currentTimeMillis() / 1000

    //从所有候选的triggerTimes中，寻找当满足如下条件的第一个元素，该元素
    //大于等于当前时间，差值作为下次被调度的时间
    val triggerDelay: Option[(Long, List[ScheduleJob])] = triggers
      .map { t => (t.toSeconds() - secondNow, t.jobs) }
      .find(_._1 >= 0)

    if (triggerDelay.nonEmpty) {
      triggerDelay
    } else {
      //没有满足条件的下一次触发时间，则说明当时所有调度都已经执行成功
      //应该进入下一天的首次调度
      val first = triggers.headOption
      if (first.isEmpty) {
        None
      } else {
        // 下一天的首次触发时间减去当前时间
        val nextTime = first.get.toSeconds() + 86400 - secondNow
        Some((nextTime, first.get.jobs))
      }
    }
  }
}
