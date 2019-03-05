package xiatian.octopus.actor

import akka.actor.{Actor, ActorIdentity, ActorRef, Identify, ReceiveTimeout}

import scala.concurrent.duration._

/**
  * 定位远程Actor的基类
  *
  * @author Tian Xia
  *         Dec 04, 2016 00:06
  */
class LookupActor(remotePath: String) extends Actor {

  sendIdentifyRequest()

  def receive = identifying

  def identifying: Actor.Receive = {
    case ActorIdentity(`remotePath`, Some(actor)) =>
      context.watch(actor)
      context.become(active(actor))
      afterActive(actor)

    case ActorIdentity(`remotePath`, None) =>
      println(s"Remote actor not available: $remotePath")

    case ReceiveTimeout => sendIdentifyRequest()

    case _ => print("->") //println("Not ready yet")
  }

  def sendIdentifyRequest(): Unit = {
    context.actorSelection(remotePath) ! Identify(remotePath)
    import context.dispatcher
    context.system.scheduler.scheduleOnce(3.seconds, self, ReceiveTimeout)
  }

  def active(actor: ActorRef): Actor.Receive = ???

  def afterActive(actor: ActorRef): Unit = ???
}
