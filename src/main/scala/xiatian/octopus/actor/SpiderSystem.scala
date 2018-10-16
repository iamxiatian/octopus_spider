package xiatian.octopus.actor

import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props}
import akka.http.scaladsl.Http.ServerBinding
import akka.routing.RoundRobinPool
import com.typesafe.config.ConfigFactory
import xiatian.octopus.actor.ActorMessage.Starting
import xiatian.octopus.actor.master._
import xiatian.octopus.common.MyConf
import xiatian.octopus.common.MyConf._
import xiatian.octopus.httpd.HttpServer

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * 全局使用的AkkaActorSystem
  *
  * @author Tian Xia
  *         School of IRM, Renmin University of China.
  *         Jul 26, 2017 16:09
  */
object SpiderSystem {

  /**
    * AkkaSystem的Master系统
    */
  object Master {

    implicit lazy val system: ActorSystem = {
      MasterDb.open()

      if (akkaMasterConfig.isEmpty) {
        println("没有指定AkkaSystem对应的配置文件，采用临时配置方式...")
        ActorSystem("MasterSystem-TEMP", MyConf.config)
      } else {
        ActorSystem("MasterSystem", akkaMasterConfig.get)
      }
    }

    implicit lazy val executionContext = system.dispatcher

    var injectorHolder = Option.empty[ActorRef]
    var fetchMasterHolder = Option.empty[ActorRef]
    var schedulerHolder = Option.empty[ActorRef]
    var bookParseHolder = Option.empty[ActorRef]

    var restBindingHolder = Option.empty[Future[ServerBinding]]

    def start(): Unit = {
      val injector = system.actorOf(Props(classOf[InjectActor], system), "injector")
      injectorHolder = Some(injector)


      //      val fetchMaster = system.actorOf(Props[FetchMasterActor], "fetchMaster")

      // 启用10个FetchMaster，轮询响应结果
      val fetchMaster =
        if (MyConf.masterRobinCount > 1) //启用缓冲池
          system.actorOf(Props[FetchMasterActor]
            .withRouter(RoundRobinPool(nrOfInstances = 5)), "fetchMaster")
        else
          system.actorOf(Props[FetchMasterActor], "fetchMaster")

      fetchMasterHolder = Some(fetchMaster)

      val scheduler = system.actorOf(Props(classOf[ScheduleActor], system), "scheduler")
      schedulerHolder = Some(scheduler)

      println("start Rest Server...")
      val restBinding = HttpServer.start()
      restBindingHolder = Some(restBinding)
      //访问一次restBinding对象，以帮助其实例化
      restBinding.isCompleted

      println("Starting Inject board task...")
      injector ! Starting

      println("Starting scheduler actor...")
      scheduler ! Starting

      println("Master started successfully.")

      //访问一次fetchMaster,以帮助其实例化
      println(s"fetcherMaster path: ${fetchMaster.path}")

      println("Master Started.")
    }

    /**
      * 关闭Master
      */
    def shutdown() = {
      if (injectorHolder.nonEmpty) {
        println("Shutting down injector...")
        injectorHolder.get ! PoisonPill
      }

      if (schedulerHolder.nonEmpty) {
        println("Shutting down scheduler...")
        schedulerHolder.get ! PoisonPill
      }

      def doRestWork() = {
        //关闭Master的同时，关闭RocksDB
        println("Close all master databases...")
        MasterDb.close()

        if (restBindingHolder.nonEmpty) {
          println("Shutting down HTTP API server...")
          restBindingHolder.get.flatMap(_.unbind())
        }

        system.terminate().onComplete {
          case Success(_) =>
            println("Master AkkaSystem was terminated successfully.")
          case Failure(e) =>
            println(s"Master AkkaSystem terminated with ERROR: $e")
        }
      }

      if (fetchMasterHolder.nonEmpty) {
        println("return bucket links...")
        BucketController.returnLinks().onComplete {
          case Success(cnts) =>
            println(s"return $cnts links")

            println("Shutting down fetchMaster...")
            fetchMasterHolder.get ! PoisonPill

            doRestWork()
          case Failure(e) =>
            e.printStackTrace()
            doRestWork()
        }
      } else {
        doRestWork()
      }
    }
  }

  object Fetcher {
    lazy val conf = ConfigFactory.parseString(
      s"""
         |akka.remote.netty.tcp.hostname = ${MyConf.fetcherHostname}
      """
        .stripMargin
    ).withFallback(MyConf.config)

    implicit lazy val akkaSystem = ActorSystem("FetcherSystem", conf)
  }

}
