package xiatian.octopus.actor.fetcher

import akka.actor.{Actor, ActorRef, ReceiveTimeout, Terminated}
import xiatian.octopus.actor.{FetchStatsReply, FetchStatsRequest, LookupActor}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
  * 统计客户端Actor
  *
  * @author Tian Xia
  *         Dec 03, 2016 23:28
  */
class StatsClientActor(remotePath: String) extends LookupActor(remotePath) {
  val requestNew = "Ask"

  val tick = context.system.scheduler.schedule(0 millis, 10000 millis, self, requestNew)

  override def active(master: ActorRef): Actor.Receive = {
    case op: FetchStatsRequest => master ! op

    case `requestNew` =>
      master ! FetchStatsRequest()

    case FetchStatsReply(msg) =>
      println("\n========================")
      println(msg)

    case Terminated(`master`) =>
      println("master terminated")
      sendIdentifyRequest()
      context.become(identifying)

    case ReceiveTimeout =>
    // ignore
    case x: Any =>
      println(s"Received $x")
  }

  override def afterActive(master: ActorRef): Unit = {
    //master ! NEW_REQUEST
  }

}
