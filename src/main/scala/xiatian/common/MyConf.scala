package xiatian.common

import java.io.File
import java.util.concurrent.ConcurrentHashMap

import com.typesafe.config.impl.Parseable
import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions}
import org.joda.time.DateTime
import org.zhinang.conf.Configuration
import org.zhinang.protocol.http.HttpClientAgent
import xiatian.common.util.Machine

import scala.collection.JavaConverters._
import scala.io.Source

/**
  *
  * @author Tian Xia
  *         Dec 02, 2016 13:20
  */
object MyConf {
  val version = BuildInfo.version

  /** AkkaSystem使用的配置类，其启动时需要指定该类 */
  var akkaMasterConfig: Option[Config] = None


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

  /**
    * 最大保留的时间值，以秒为单位；例如，抓取时间超过该数值的链接将会抛弃掉
    */
  val MaxTimeSeconds = DateTime.parse("2999-01-01").getMillis / 1000


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

  def getString(path: String) = config.getString(path)

  def getString(path: String, defaultValue: String) =
    if (config.hasPath(path)) config.getString(path) else defaultValue

  def getInt(path: String) = config.getInt(path)

  def getBoolean(path: String) = config.getBoolean(path)

  //采集结果保存到的数据库类型
  lazy val dbTypes = getString("db.type").split(",").map(_.trim.toLowerCase).toSet
  lazy val mongoUrl = getString("db.mongo.url")
  lazy val mongoDbName = getString("db.mongo.dbName")

  lazy val elasticSearchHttpUrl = getString("db.elasticSearch.http.url")
  lazy val masterHostname = getString("master.hostname")
  lazy val masterPort = getInt("master.port")
  lazy val masterRobinCount = getInt("master.robinCount")
  lazy val masterDbPath = new File(getString("master.db.path"))
  masterDbPath.mkdirs()

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

  /**
    * 获取爬虫的标记名称
    */
  val fetcherId: String = Machine.getLocalIp.getOrElse("127.0.0.1")

  //作品每次注入的时间间隔
  val workInjectInterval = getInt("book.work.inject.interval")
  //val workSyncInterval = getInt("book.work.sync.interval")

  // 作者头像目录
  lazy val avatarAuthorDir = new File(getString("book.avatar.author.dir"))

  // Bing学术搜索的入口
  lazy val bingAcademicPoint = getString("book.bing.academic.point")
  lazy val amazonSearchPoint = getString("book.amazon.search.point")

  //存放图书的位置
  lazy val bookFtpHost = MyConf.getString("book.ftp.host")
  lazy val bookFtpPort = MyConf.getInt("book.ftp.port")
  lazy val bookFtpUser = MyConf.getString("book.ftp.user")
  lazy val bookFtpPassword = MyConf.getString("book.ftp.password")
  lazy val bookFtpPrefix = MyConf.getString("book.ftp.prefix")


  //CoreNLPs所在的计算机地址
  lazy val nlpServerHost = {
    val host = getString("nlp.stanford.host")
    if (!host.toLowerCase().startsWith("http")) s"http://$host" else host
  }
  lazy val nlpServerPort = getInt("nlp.stanford.port")
  lazy val nlpAnnotators = getString("nlp.stanford.annotators")

  lazy val opennlpModelDir = new File(getString("nlp.apache.model.dir"))

  def outputConfig() = println(screenConfigText)


  val screenConfigText: String = {
    val mysqlConfigLines =
      if (dbTypes.contains("mysql"))
        List(s"msyql url ==> ${getString("db.book.mysql.url")}",
          s"msyql driver ==> ${getString("db.book.mysql.driver")}",
          s"msyql user ==> ${getString("db.book.mysql.user")}",
          s"msyql password ==> ${getString("db.book.mysql.password")}"
        )
      else List.empty[String]

    val mongoConfigLines =
      if (dbTypes.contains("mongodb"))
        List(
          s"mongo url ==> ${mongoUrl}",
          s"mongo splitCollection ==> ${splitCollection}"
        )
      else List.empty[String]


    val pgConfigLines =
      if (dbTypes.contains("es"))
        List(
          s"postgres url ==> ${getString("db.book.pg.url")}",
          s"postgres driver ==> ${getString("db.book.pg.driver")}",
          s"postgres user ==> ${getString("db.book.pg.user")}",
          s"postgres password ==> ${getString("db.book.pg.password")}"
        )
      else List.empty[String]

    val dbConfigLines = mysqlConfigLines ::: mongoConfigLines ::: pgConfigLines

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
       |├── injector config:
       |│   └── work interval seconds ==> ${workInjectInterval}
       |│
       |├── NLP config:
       |│   ├── apache opennlp model dir ==> ${opennlpModelDir.getCanonicalPath}
       |│   ├── stanford nlp server host ==> $nlpServerHost
       |│   ├── stanford nlp server port ==> $nlpServerPort
       |│   └── stanford nlp annotators ==> $nlpAnnotators
       |│
       |├── Book config:
       |│   ├── Amazon search point ==> $amazonSearchPoint
       |│   ├── Bing academic point ==> $bingAcademicPoint
       |│   ├── FTP host ==> $bookFtpHost
       |│   ├── FTP port ==> $bookFtpPort
       |│   ├── FTP user ==> $bookFtpUser
       |│   ├── FTP password ==> $bookFtpPassword
       |│   ├── FTP prefix ==> $bookFtpPrefix
       |│   └── author avatar dir ==> ${avatarAuthorDir.getCanonicalPath}
       |│
       |├── scheduler config:
       |${lineToString(scheduleConfigLines)}
       |│
       |├── database config:
       |${lineToString(dbConfigLines)}
       |│
       |├── speed control list:
       |${lineToString(speedConfigLines)}
       |│
       |└── Crawling configuration:
       |${lineToString(crawlConfigLines, true)}
       |
       |""".stripMargin
  }

}