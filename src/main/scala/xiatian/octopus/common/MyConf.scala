package xiatian.octopus.common

import java.io.{File, FileInputStream}
import java.util.concurrent.ConcurrentHashMap

import com.typesafe.config.impl.Parseable
import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions}
import org.joda.time.DateTime
import org.zhinang.conf.Configuration
import org.zhinang.protocol.http.HttpClientAgent
import xiatian.octopus.util.Machine

import scala.collection.JavaConverters._
import scala.io.Source

/**
  *
  * @author Tian Xia
  *         Dec 02, 2016 13:20
  */
object MyConf {
  lazy val httpUserAgent = getString("fetcher.http.user.agent")
  lazy val httpConnectionTimeout = getInt("fetcher.http.connection.timeout")
  lazy val httpSocketTimeout = getInt("fetcher.http.socket.timeout")
  lazy val zhinangConf = new Configuration()
    .set("http.user.agent", httpUserAgent)
    .setInt("http.connection.timeout", httpConnectionTimeout)
    .setInt("http.socket.timeout", httpSocketTimeout)
    .setInt("http.proxy.port", 0) //默认不启动代理
  //先采用my.conf中的配置，再使用application.conf中的默认配置
  lazy val config: Config = {
    val confFile = new File("./conf/my.conf")

    //先采用conf/my.conf中的配置，再使用application.conf中的默认配置
    if (confFile.exists()) {
      println(s"启用配置文件${confFile.getCanonicalPath}")
      val unresolvedResources = Parseable
        .newResources("application.conf", ConfigParseOptions.defaults())
        .parse()
        .toConfig()

      ConfigFactory.parseFile(confFile).withFallback(unresolvedResources).resolve()
    } else {
      ConfigFactory.load()
    }
  }
  //文章链接的停用词，如果文章锚文本与其中的词语相同，则过滤掉该链接
  lazy val articleStopWords: Set[String] =
    Source.fromFile(
      "conf/stopwords4article.txt",
      "utf-8"
    ).getLines().filter(!_.trim.isEmpty).toSet
  //根据域名进行限速的Map，主键为域名，值为以秒为单位的时间间隔
  lazy val speedControlMap = {
    def isNumeric(maybeNumeric: String): Boolean =
      maybeNumeric != null && maybeNumeric.matches("[0-9]+")

    val map = new ConcurrentHashMap[String, (Int, Int)]()
    val f = new java.io.File("./conf/speed-control.txt")
    if (f.exists()) {
      Source.fromFile("./conf/speed-control.txt", "utf-8")
        .getLines()
        .filter(line => line.trim.nonEmpty &&
          (!line.contains("#")) &&
          (line.contains("\t") || line.contains(" ")))
        .foreach {
          line =>
            val parts: Array[String] = line.split("\t| |,").filter(_.nonEmpty)
            if (parts.size < 2 ||
              !isNumeric(parts(1)) ||
              (parts.size == 3 && !isNumeric(parts(2)))) {
              println("Invalid line in speed-control.txt ==> $line")
            }

            map.put(parts(0), (
              parts(1) toInt,
              if (parts.length > 2) parts(2).toInt else parts(1).toInt
            ))
        }
    } else {
      println("./conf/speed-control.txt does not exist.")
    }

    map
  }


  lazy val masterHostname = getString("master.hostname")
  lazy val masterPort = getInt("master.port")
  lazy val masterRobinCount = getInt("master.robinCount")
  lazy val masterDbPath = new File(getString("master.db.path"))
  lazy val masterDbCacheSize = getInt("master.db.cacheSize")
  lazy val maxLinkDepth = getInt("master.link.max.depth")
  lazy val maxLinkRetries = getInt("master.link.max.retries")
  lazy val apiServerPort = getInt("master.http.port")
  lazy val fetcherHostname = getString("fetcher.hostname")
  lazy val splitCollection = getBoolean("db.mongo.splitCollection")
  lazy val articleMustContains =
    if (config.hasPath("article.mustContainsRegex")) {
      val regex = getString("article.mustContainsRegex")
      if (regex == "") None else Some(regex.r)
    } else None
  masterDbPath.mkdirs()

  lazy val articleCheckDuplicate = getBoolean("article.checkDuplicate")
  lazy val saveHtmlFormat = getBoolean("article.saveHtmlFormat")

  /**
    * 返回触发的时间点（每一个时间点都是一对整数： (小时, 分钟)）
    */
  lazy val mailTriggerTimes: List[(Int, Int)] =
    getString("scheduler.mail.triggerTimes")
      .split(" ")
      .map {
        t =>
          val parts = t.split(":").map(_ toInt)
          (parts(0), parts(1))
      }.toList

  lazy val mailNotify = getBoolean("scheduler.mail.notify") //是否启用邮件通知

  val version = BuildInfo.version

  /**
    * 最大保留的时间值，以秒为单位；例如，抓取时间超过该数值的链接将会抛弃掉
    */
  val MaxTimeSeconds = DateTime.parse("2999-01-01").getMillis / 1000

  /**
    * 获取爬虫的标记名称
    */
  val fetcherId: String = Machine.getLocalIp.getOrElse("127.0.0.1")

  val screenConfigText: String = {

    val scheduleConfigLines = {
      if (mailNotify) {
        List(
          s"mail.triggerTimes ==> $mailTriggerTimes",
          s"mail.notify ==> $mailNotify"
        )
      } else
        List(
          s"mail.triggerTimes ==> $mailTriggerTimes",
          s"mail.notify ==> $mailNotify",
          s"mail.smtp.host ==> ${getString("scheduler.mail.smtp.host")}",
          s"mail.smtp.port ==> ${getInt("scheduler.mail.smtp.port")}",
          s"mail.smtp.user ==> ${getString("scheduler.mail.smtp.user")}",
          s"mail.smtp.auth ==> ${getBoolean("scheduler.mail.smtp.auth")}",
          s"mail.smtp.startTtls ==> ${getBoolean("scheduler.mail.smtp.startTtls")}",
          s"mail.receivers ==> ${getString("scheduler.mail.receivers")}"
        )
    }

    lazy val speedConfigLines = speedControlMap.asScala.map {
      case (host, limits) =>
        s"$host ==> [${limits._1}, ${limits._2}]"
    }.toList

    val crawlConfigLines = HttpClientAgent.getConfigMessage(zhinangConf)
      .split("\n")
      .toList
      .map {
        _.replace("=", "==>").replaceAll("\t", " ").trim
      }

    def lineToString(lines: List[String], lastPart: Boolean = false): String = {
      val text = lines.zipWithIndex.map {
        case (line, idx) =>
          if (idx == lines.size - 1) {
            if (lastPart) s"    └── ${line}" else s"│   └── ${line}"
          } else {
            if (lastPart) s"    ├── ${line}" else s"│   ├── ${line}"
          }
      }.mkString("\n")

      if (text.isEmpty) {
        if (lastPart) "" else "│"
      } else text
    }

    s"""
       |My configuration(build: ${BuildInfo.builtAtString}):
       |├── master config:
       |│   ├── hostname ==> $masterHostname
       |│   ├── port ==> $masterPort
       |│   ├── masterDbPath==>${masterDbPath.getCanonicalPath}
       |│   ├── masterDbCacheSize==>${masterDbCacheSize}
       |│   ├── max link depth ==>${maxLinkDepth}
       |│   ├── max link retries ==>${maxLinkRetries}
       |│   └── API http port ==> ${apiServerPort}
       |│
       |├── fetcher config:
       |│   ├── fetcher identification ==> $fetcherId
       |│   ├── hostname ==> $fetcherHostname
       |│   ├── userAgent ==> $httpUserAgent
       |│   ├── connectionTimeout ==> $httpConnectionTimeout
       |│   ├── socketTimeout ==> $httpSocketTimeout
       |│   ├── numOfFetchClientActors ==> ${getInt("fetcher.numOfFetchClientActors")}
       |│   └── parseDataPageLinks ==> ${getBoolean("fetcher.parseDataPageLinks")}
       |│
       |├── scheduler config:
       |${lineToString(scheduleConfigLines)}
       |│
       |├── speed control list:
       |${lineToString(speedConfigLines)}
       |│
       |└── Crawling configuration:
       |${lineToString(crawlConfigLines, true)}
       |
       |""".stripMargin
  }

  /** AkkaSystem使用的配置类，其启动时需要指定该类 */
  var akkaMasterConfig: Option[Config] = None

  def getString(path: String) = config.getString(path)

  def getString(path: String, defaultValue: String) =
    if (config.hasPath(path)) config.getString(path) else defaultValue

  def getInt(path: String) = config.getInt(path)

  def getBoolean(path: String) = config.getBoolean(path)

  def outputConfig() = println(screenConfigText)

  def configLog(): Unit = {
    val f = new File("./conf/logback.xml")
    if (f.exists()) {
      import ch.qos.logback.classic.LoggerContext
      import ch.qos.logback.classic.joran.JoranConfigurator
      import org.slf4j.LoggerFactory
      val loggerContext = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
      loggerContext.reset()
      val configurator = new JoranConfigurator
      val configStream = new FileInputStream(f)
      configurator.setContext(loggerContext)
      configurator.doConfigure(configStream) // loads logback file
      configStream.close()
      println("finished to config logback.")
    }
  }
}