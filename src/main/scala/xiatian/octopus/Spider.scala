package xiatian.octopus

import java.io.File
import java.util.Date

import akka.actor.{ActorSystem, PoisonPill, Props}
import com.typesafe.config.ConfigFactory
import org.joda.time.DateTime
import xiatian.octopus.actor.SpiderSystem
import xiatian.octopus.actor.SpiderSystem.Master
import xiatian.octopus.actor.fetcher.{FetchClientActor, StatsClientActor}
import xiatian.octopus.actor.master.UrlManager
import xiatian.octopus.common.{BuildInfo, MyConf}
import xiatian.octopus.inject.MonitorKeyword

/**
  *
  * @author Tian Xia
  *         Dec 03, 2016 22:44
  */
object Spider extends App {

  case class Config(master: Boolean = false,
                    fetcher: Boolean = false,
                    injectXmlFile: Option[String] = None,
                    injectKeywordFile: Option[String] = None,
                    license: Boolean = false,
                    clear: Boolean = false,
                    version: Boolean = false,
                    test: Boolean = false,
                    fromDate: Option[Date] = None,
                    endDate: Option[Date] = None,
                    outFile: Option[File] = None
                   )

  val parser = new scopt.OptionParser[Config]("bin/spider") {
    head("Web Site Crawler", "1.0")

    import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

    val format: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")

    opt[Unit]("version").action((_, c) =>
      c.copy(version = true)).text("Current version and build info.")

    opt[Unit]("master").action((_, c) =>
      c.copy(master = true)).text("Start master.")

    opt[Unit]("fetcher").action((_, c) =>
      c.copy(fetcher = true)).text("Start to fetch pages.")

    opt[Unit]("test").action((_, c) =>
      c.copy(test = true)).text("临时测试使用的参数.")

    opt[Unit]("license").action((_, c) =>
      c.copy(license = true)).text("License information.")

    opt[Unit]("clear").action((_, c) =>
      c.copy(clear = true)).text("Clear all REDIS tasks and fetch urls.")

    opt[String]('x', "injectXml").optional().
      action((x, c) => c.copy(injectXmlFile = Some(x))).
      text("Inject tasks from xml file.")

    opt[String]('k', "injectKeyword").optional().
      action((x, c) => c.copy(injectKeywordFile = Some(x))).
      text("Inject monitoring keywords from text file.")

    opt[String]("from")
      .optional()
      .action(
        (x, c) =>
          c.copy(fromDate = Some(DateTime.parse(x, format).toDate)))
      .text("From Date with format YYYY-MM-dd, like: 2017-08-22")

    opt[String]("end")
      .optional()
      .action(
        (x, c) =>
          c.copy(endDate = Some(DateTime.parse(x, format).toDate)))
      .text("End Date with format YYYY-MM-dd, like: 2017-08-26")

    opt[String]('o', "out")
      .optional()
      .action(
        (x, c) =>
          c.copy(outFile = Some(new File(x))))
      .text("output file name")

    help("help").text("prints this usage text")

    note("\n xiatian, xia@ruc.edu.cn.")
  }

  MyConf.configLog()
  MyConf.outputConfig()

  parser.parse(args, Config()) match {
    case Some(config) =>
      if (config.version) {
        println("Build information:")
        BuildInfo.toMap.toSeq.sortBy(_._1).foreach {
          case (k, v) =>
            println(s"\t${k}: \t $v")
        }
      }

      if (config.master) startMasterSystem()

      if (config.fetcher) startFetchSystem()

      if (config.clear) {
        println(s"Remove all tasks and urls...")
        UrlManager.clear()
        println("Finished!")
      }

      if (config.injectXmlFile.isDefined) {
        if (config.test) {
          //尝试把xml文件的内容以任务形式POST给HTTP API接口
          //Board.testPostTask(config.injectXmlFile.get, "utf-8")
          println("Finished.")
        } else {
          println(s"Inject xml task from file ${config.injectXmlFile.get}")
          SpiderSystem.Master.shutdown()
        }
      }

      if (config.injectKeywordFile.isDefined) {
        SpiderSystem.Master.system

        println(s"Inject keyword task from file ${config.injectKeywordFile.get}")
        MonitorKeyword.injectFromFile(config.injectKeywordFile.get)
        SpiderSystem.Master.shutdown()
      }

    case None => {
      println(
        """
          |Wrong parameters :(
          |Try following command,
          |
          | Start master:
          | bin/spider --master
          |
          |Start fetcher:
          |bin/spider --fetcher
          |
          |Show version information:
          |bin/spider --version
        """.stripMargin)
      //      startMasterSystem()
      //      startFetchSystem()
    }
  }

  def startMasterSystem(): Unit = {
    val conf = ConfigFactory.parseString(
      s"""
         |akka.remote.netty.tcp.hostname = ${MyConf.masterHostname}
         |akka.remote.netty.tcp.port = ${MyConf.masterPort}
      """
        .stripMargin
    ).withFallback(MyConf.config)

    //把conf对象传递给Settings，再有AkkaSystem.Master引用，继续后面的实例化过程
    MyConf.akkaMasterConfig = Some(conf)

    val servicePoint = s"${conf.getString("akka.remote.netty.tcp.hostname")}" +
      s":${conf.getString("akka.remote.netty.tcp.port")}"

    println(s"Master will run at $servicePoint")

    Master.start()

    sys.addShutdownHook {
      Master.shutdown()
      println("DONE.")
    }
  }

  def startFetchSystem(): Unit = {
    val conf = SpiderSystem.Fetcher.conf
    val system = SpiderSystem.Fetcher.akkaSystem

    val numOfFetchClientActors = conf.getInt("fetcher.numOfFetchClientActors")

    //"akka.tcp://MasterSystem@127.0.0.1:2552/user/fetchMaster"
    val fetchMasterRemotePath = s"akka.tcp://MasterSystem@${MyConf.masterHostname}:${MyConf.masterPort}/user/fetchMaster"

    val servicePoint = s"${conf.getString("akka.remote.netty.tcp.hostname")}:${conf.getString("akka.remote.netty.tcp.port")}"
    println(s"starting Fetcher at $servicePoint, target master: $fetchMasterRemotePath")

    val fetchClients = (0 until numOfFetchClientActors).map(idx => system.actorOf(Props(classOf[FetchClientActor], fetchMasterRemotePath, idx), name = s"fetchClient-" + idx))

    println("Started FetchSystem")

    sys.addShutdownHook {
      println("shutdown...")
      fetchClients.foreach(_ ! PoisonPill)

      system.terminate
      println("DONE.")
    }
  }

  /**
    * 启动查看抓取状态报告信息的客户端，启动后尝试链接FetchMaster，一旦链接上，则开始轮询获取统计报告信息
    */
  def startReportSystem(): Unit = {
    val fetchMasterRemotePath = s"akka.tcp://MasterSystem@${MyConf.masterHostname}:${MyConf.masterPort}/user/fetchMaster"

    val system = ActorSystem("ReportSystem", MyConf.config)

    val reportClient = system.actorOf(Props(classOf[StatsClientActor], fetchMasterRemotePath), name = s"reportClient")
    println("Started Report Client")

    sys.addShutdownHook {
      println("shutdown...")
      reportClient ! PoisonPill
      system.terminate
      println("DONE.")
    }
  }

}
