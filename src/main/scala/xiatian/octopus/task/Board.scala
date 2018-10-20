package xiatian.octopus.task

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.apache.commons.lang3.math.NumberUtils
import org.slf4j.LoggerFactory
import xiatian.octopus.FastSerializable
import xiatian.octopus.actor.master.{BucketController, MasterConfig, UrlManager}
import xiatian.octopus.common.MyConf
import xiatian.octopus.model.{FetchLink, HubLink}
import xiatian.octopus.storage.master.BoardDb
import xiatian.octopus.util.TryWith

import scala.collection.mutable.ListBuffer
import scala.xml.XML


/**
  *
  * @author Tian Xia
  *         Nov 27, 2016 17:08
  */


/**
  * 抓取的板块定义，板块定义了爬虫的入口地址、文章url正则表达式，最大抓取深度等
  *
  */
case class Board(code: String, // 频道编号（必需，每个频道的编号唯一）
                 actionType: Int, // 任务状态（缺省为：0  0-正常，1-新加任务，2-修改任务，3-删除任务）
                 name: String, // 频道名称
                 boardType: Int, // 频道类型（其含义由应用层解释，与采集器内部机制无关）, 1为定向采集任务，2为全网采集任务
                 typeId: Int, // 通道类型ID(1-新闻,2-论坛,3-博客,4-贴吧,5-网页)
                 infoId: Int, // 所属频道分组的编号
                 siteCode: Int, //所属站点的编号
                 siteAreaCode: Int, //地区代码
                 siteKindCode: Int, //媒体类型代码
                 siteWeight: Int, //媒体的重要性，0到100之间的数值，默认为0
                 language: String, //语种
                 entryUrls: Seq[String], // 入口URL（必须以http://或者https://开始）
                 entryType: String, // 入口URL类型,0:一般URL，1:RSS, 2:WAP, 缺省为0
                 charset: String, // 入口网页编码类型，不设置时为AUTO（取值范围：AUTO/GB2312/GBK/GB18030/UTF-8/UTF-7/UNICODE）
                 articleCharset: String, //文章网页编码类型（此参数不存在或未设置具体值时，同入口网页）
                 fan2Jian: Boolean, //是否进行繁简体转换（缺省为0）
                 cookie: String, //设置cookie，缺省为空
                 javascript: String,
                 useProxy: Boolean, // 是否使用代理采集本频道
                 extractorType: String, //网页抽取类型：0-通用抽取；1-模板抽取
                 extratorFile: String, // 抽取模板文件名（该文件与boards.xml处于同一文件夹）
                 infoCheck: String, //是否需要审核
                 articleFilter: ArticleFilter,
                 image: ImageRule,
                 gatherDepth: Int, //采集深度
                 updateMinutes: Int, //刷新周期（单位：分钟）
                 reqTimeInterval: Int, //请求时间间隔（单位：秒，缺省为0）
                 nextPageUrlRule: String, //分页URL判定规则
                 maxNextPageCount: Int, //最大翻页数
                 gatherDayNum: Int //采集多长时间的内容，单位为天数, 默认为2
                ) extends FastSerializable {
  def isArticle(fetchLink: FetchLink) = {
    val url = fetchLink.url
    val flag = articleFilter.urlRule.r.pattern.matcher(url).matches() && !articleFilter.notUrlRule.r.pattern.matcher(url).matches()
  }

  def toBytes(): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val dos = new DataOutputStream(out)
    dos.writeUTF(MyConf.version)
    dos.writeUTF(code)
    dos.writeInt(actionType)
    dos.writeUTF(name)
    dos.writeInt(boardType)
    dos.writeInt(typeId)
    dos.writeInt(infoId)
    dos.writeInt(siteCode)
    dos.writeInt(siteAreaCode)
    dos.writeInt(siteKindCode)
    dos.writeInt(siteWeight)
    dos.writeUTF(language)

    dos.writeInt(entryUrls.size)
    entryUrls.foreach(dos.writeUTF)

    dos.writeUTF(entryType)
    dos.writeUTF(charset)

    if (articleCharset.isEmpty) {
      dos.writeUTF(charset)
    } else {
      dos.writeUTF(articleCharset)
    }

    dos.writeBoolean(fan2Jian)
    dos.writeUTF(if (cookie == null) "" else cookie)
    dos.writeUTF(if (javascript == null) "" else javascript)

    dos.writeBoolean(useProxy)
    dos.writeUTF(if (extractorType == null) "0" else extractorType)
    dos.writeUTF(if (extratorFile == null) "" else extratorFile)
    dos.writeUTF(if (infoCheck == null) "" else infoCheck)

    articleFilter.writeBytes(dos)
    image.writeBytes(dos)

    dos.writeInt(gatherDepth)
    dos.writeInt(updateMinutes)
    dos.writeInt(reqTimeInterval)

    dos.writeUTF(if (nextPageUrlRule == null) "" else nextPageUrlRule)

    dos.writeInt(maxNextPageCount)
    dos.writeInt(gatherDayNum)

    dos.close()
    out.close()

    out.toByteArray
  }
}

/**
  * 默认的Board，如果一个链接指定的boardId不存在，则采用该实例
  */

sealed case class ImageRule(flag: Boolean = false, //是否下载图片（当前只支持提取图片URL），为0时ImgUrlRule参数无效
                            inText: Boolean = true, //图片从正文中提取还是网页中提取，为0时从网页中提取
                            imgUrlRule: String = ".*\\.jpg" // #图片URL判定规则（不区分大小写）
                            //imgUrlRule: Regex = ".*\\.jpg".r // #图片URL判定规则（不区分大小写）
                           ) extends FastSerializable {
  def toBytes(): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val dos = new DataOutputStream(out)

    writeBytes(dos)

    dos.close()
    out.close()

    out.toByteArray
  }

  def writeBytes(dos: DataOutputStream): Unit = {
    dos.writeBoolean(flag)
    dos.writeBoolean(inText)
    dos.writeUTF(imgUrlRule)
  }
}

object ImageRule {
  def readFrom(din: DataInputStream) =
    new ImageRule(
      din.readBoolean(),
      din.readBoolean(),
      din.readUTF()
    )
}

//val s = "http://[\\x00-\\xff]*[0-9]*[\\x00-\\xff]*".r
//s.findFirstIn("http://www.baidu.com/kdsfldskfa")

case class ArticleFilter(urlRule: String, //文章URL判定规则（不区分大小写）
                         notUrlRule: String, //文章URL排除规则
                         minAnchorLength: Int = 10, // 锚文本最小长度（字节数）
                         minTextLength: Int = 100, //正文最小长度（字节数）
                         matcher: StringMatcher,
                         stopFilter: StopFilter
                        ) extends FastSerializable {
  def toBytes(): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val dos = new DataOutputStream(out)

    writeBytes(dos)

    dos.close()
    out.close()

    out.toByteArray
  }

  def writeBytes(dos: DataOutputStream): Unit = {
    dos.writeUTF(urlRule)
    dos.writeUTF(if (notUrlRule == null) "" else notUrlRule)
    dos.writeInt(minAnchorLength)
    dos.writeInt(minTextLength)

    matcher.writeBytes(dos)
    stopFilter.writeBytes(dos)
  }
}

object ArticleFilter {
  def readFrom(din: DataInputStream) =
    new ArticleFilter(
      din.readUTF(),
      din.readUTF(),
      din.readInt(),
      din.readInt(),
      StringMatcher.readFrom(din),
      StopFilter.readFrom(din)
    )
}

sealed case class StringMatcher(enabled: Boolean = false, // 是否进行关键词过滤（0-否；1-是；缺省为0；为0时以下几项无效）
                                keywordsFile: String = "", //过滤关键词文件
                                minDist: Int = 0, //最小词间距离阈值（缺省为0时此机制不起作用）
                                field: Int = 0, //最小词间距离阈值（缺省为0时此机制不起作用）
                                hitHandler: Int = 2, //匹配时的处理策略（缺省为0：0-入库（不需审核）；1-入库（需审核）；2-不入库）
                                missHandler: Int = 2 //未匹配时的处理策略（缺省为0：0-入库（不需审核）；1-入库（需审核）；2-不入库）
                               ) extends FastSerializable {
  def toBytes(): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val dos = new DataOutputStream(out)

    writeBytes(dos)

    out.toByteArray
  }

  def writeBytes(dos: DataOutputStream): Unit = {
    dos.writeBoolean(enabled)
    dos.writeUTF(if (keywordsFile == null) "" else keywordsFile)
    dos.writeInt(minDist)
    dos.writeInt(field)
    dos.writeInt(hitHandler)
    dos.writeInt(missHandler)
  }
}

object StringMatcher {
  def readFrom(din: DataInputStream) =
    new StringMatcher(
      din.readBoolean(),
      din.readUTF(),
      din.readInt(),
      din.readInt(),
      din.readInt(),
      din.readInt()
    )
}

sealed case class StopFilter(enabled: Boolean = false, //是否进行停用词过滤（缺省为0）
                             stopwordsFile: String = "", //停用词文件，一行一个词。若Field指定字段中包含该文件中的任何一个词，该文将抛弃
                             field: Int = 1 //过滤哪个字段（缺省为1：0-所有字段；1-标题；2-正文）
                            ) extends FastSerializable {
  def toBytes(): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val dos = new DataOutputStream(out)

    writeBytes(dos)

    dos.close()
    out.close()

    out.toByteArray
  }

  def writeBytes(dos: DataOutputStream): Unit = {
    dos.writeBoolean(enabled)
    dos.writeUTF(if (stopwordsFile == null) "" else stopwordsFile)
    dos.writeInt(field)
  }
}

object StopFilter {
  def readFrom(din: DataInputStream) =
    new StopFilter(
      din.readBoolean(),
      din.readUTF(),
      din.readInt()
    )
}

/**
  * Board的伴生对象
  */
object Board extends MasterConfig {
  val log = LoggerFactory.getLogger(Board.getClass)
  /**
    * 没有对应的频道的默认类
    */
  val noneBoard = Board("0",
    0,
    "UNKNOWN",
    1, 0, 0, 0, 0, 0, 1, "UNKNOWN", List("#"),
    "0", "AUTO", "AUTO", false, "", "", false, "0", "", "infoCheck",
    //ArticleFilter(".*", ".*", 10, 100, StringMatcher(), StopFilter()),
    ArticleFilter("removed_links", ".*", 10, 100, StringMatcher(), StopFilter()),
    ImageRule(),
    1, Int.MaxValue,
    0,
    "removed_links",
    0,
    2)

  def readFrom(bytes: Array[Byte]) = {
    val din = new DataInputStream(new ByteArrayInputStream(bytes))

    din.readUTF() //version info

    val code = din.readUTF()
    val actionType = din.readInt()
    val name = din.readUTF()
    val boardType = din.readInt()
    val typeID = din.readInt()
    val infoId = din.readInt()
    val siteCode = din.readInt()
    val siteAreaCode = din.readInt()
    val siteKindCode = din.readInt()
    val siteWeight = din.readInt()
    val language = din.readUTF()

    val size = din.readInt()
    val entryUrls: Seq[String] = (1 to size) map {
      _ => din.readUTF()
    }

    val entryType = din.readUTF()
    val charset = din.readUTF()
    val articleCharset = din.readUTF()

    val fan2Jian = din.readBoolean()
    val cookie = din.readUTF()
    val javascript = din.readUTF()

    val useProxy = din.readBoolean()
    val extractorType = din.readUTF()
    val extratorFile = din.readUTF()
    val infoCheck = din.readUTF()

    val articleFilter = ArticleFilter.readFrom(din)
    val image = ImageRule.readFrom(din)

    val gatherDepth = din.readInt()
    val updateMinutes = din.readInt()
    val reqTimeInterval = din.readInt()

    val nextPageUrlRule = din.readUTF()

    val maxNextPageCount = din.readInt()
    val gatherDayNum = din.readInt()

    din.close()

    new Board(code,
      actionType,
      name,
      boardType,
      typeID,
      infoId,
      siteCode,
      siteAreaCode,
      siteKindCode,
      siteWeight,
      language,
      entryUrls,
      entryType,
      charset,
      articleCharset,
      fan2Jian,
      cookie,
      javascript,
      useProxy,
      extractorType,
      extratorFile,
      infoCheck,
      articleFilter,
      image,
      gatherDepth,
      updateMinutes,
      reqTimeInterval,
      nextPageUrlRule,
      maxNextPageCount,
      gatherDayNum)
  }

  def toLong(value: String) = NumberUtils.toLong(value, 0)

  def toLong(value: String, errorMsg: String) = {
    if (!NumberUtils.isNumber(value))
      log.error(errorMsg)
    NumberUtils.toLong(value, 0)
  }

  /**
    * 把一个频道规则，注入到Redis中, 如果所有的记录都设置成功，则返回Futre[True]，
    * 否则返回Future[False]
    *
    * @param crawlNow 如果设为true，则同时把该频道构造为FetchLink，
    *                 注入到内存中的队列桶中
    */
  def injectBoard(board: Board, crawlNow: Boolean = false): Boolean =
    if (board.actionType == 3) {
      log.info("try to delete ${board.code} because actionType = 3")
      BoardDb.delete(board.code) //actionType = 3表示要删除该任务
      true
    } else {
      BoardDb.save(board)
      log.info(s"push link to crawling queue: ${board.entryUrls.mkString("\t")}")
      if (crawlNow) {
        board.entryUrls.map(
          entryUrl =>
            BucketController.fillLink(
              FetchLink(entryUrl, None, board.name, 0, 0, HubLink, board.code),
              true
            )
        )
      }

      board.entryUrls.map(
        entryUrl =>
          UrlManager.pushLink(
            FetchLink(entryUrl, None, board.name, 0, 0, HubLink, board.code),
            false
          )
      )
      true
    }

  def entryLinks(): Seq[FetchLink] = entryLinks(0, BoardDb.count())

  /**
    * 返回从start开始到end截至（包括）d的频道对应的FetchLink对象
    *
    * @param startInclude
    * @param endInclude
    * @return
    */
  def entryLinks(startInclude: Int, endInclude: Int): Seq[FetchLink] =
    BoardDb.getBoardIds(startInclude, endInclude).flatMap {
      id =>
        BoardDb.get(id).map {
          board =>
            board.entryUrls.map {
              url =>
                FetchLink(url, None, board.name, 0, 0, HubLink, id)
            }
        }
    }.flatten

  def count(): Int = BoardDb.count()

  def injectFromXml(xmlFile: String, encoding: String = "utf-8"): Unit = {
    println(s"Injecting boards to Redis: ")
    loadBoardsFromXml(xmlFile, encoding).foreach {
      board =>
        BoardDb.save(board)
        print(".")
    }
    println("\tDONE.")
  }

  def loadBoardsFromXml(xmlFile: String, encoding: String): List[Board] = {
    println(s"Loading xml file from " +
      s"${new java.io.File(xmlFile).getCanonicalPath} " +
      s"with encoding $encoding ...")

    val boardBuffer = ListBuffer[Board]()

    TryWith(
      scala.io.Source.fromFile(xmlFile, encoding)
    ) {
      source =>
        val lines = source.getLines()

        var count = 1
        while (lines.hasNext) {
          val boardText = lines.dropWhile(!_.trim.startsWith("<BOARD>"))
            .takeWhile(!_.trim.startsWith("</BOARD>"))
            .map { s =>
              val pos1 = s.lastIndexOf(">")
              val pos2 = s.lastIndexOf("#")
              if (pos2 > 0 && pos2 > pos1)
                filterUnicodeString(s.substring(0, pos2))
              else
                filterUnicodeString(s)
            }
            .mkString("\n")

          if (boardText.length > 0) {
            val board = parseBoardText(boardText + "\n</BOARD>")

            if (count % 500 == 0)
              println(s"$count \t loading BOARD ${board.name} (${board.code})")
            count += 1

            boardBuffer.append(board)
          }
        }
    }

    println(s"Load ${boardBuffer.length} boards from " +
      s"${new java.io.File(xmlFile).getCanonicalPath}")

    boardBuffer.toList
  }

  /**
    * 过滤掉特殊的非法XML字符，如0x13. 这些无效的字符在一些文档中作为文档处理器的控制编码（微软选择了那些再0x82到0x95之间的字符作为"smart"标点），
    * 这些也被Unicode保留作为控制编码的，并且在XML中是不合法的。
    */
  def filterUnicodeString(value: String) = {
    val chs = value.toArray
    for (i <- 0 until value.length()) {
      if (chs(i) > 0xFFFD) {
        chs(i) = ' ';
      }
      else if (chs(i) < 0x20 && chs(i) != '\t' & chs(i) != '\n' & chs(i) != '\r') {
        chs(i) = ' ';
      }
      else if (chs(i) >= 0x80 && chs(i) <= 0x9f) {
        chs(i) = ' ';
      }
    }
    new String(chs);
  }

  def parseBoardText(boardText: String): Board = {
    val boardElement = XML.loadString(boardText)

    Board(
      (boardElement \ "Code").text.trim,
      toInt((boardElement \ "ActionType").text.trim),
      (boardElement \ "Name").text.trim,
      toInt((boardElement \ "Board_Type").text.trim),
      toInt((boardElement \ "TypeID").text.trim),
      toInt((boardElement \ "infoid").text.trim),
      toInt((boardElement \ "SiteCode").text.trim),
      toInt((boardElement \ "SiteAreaCode").text.trim),
      toInt((boardElement \ "SiteKindCode").text.trim),
      toInt((boardElement \ "SiteWeight").text.trim),
      (boardElement \ "Language").text.trim,
      (boardElement \ "EntryUrl").map(_.text.trim),
      (boardElement \ "EntryType").text.trim,
      (boardElement \ "Charset" text),
      (boardElement \ "ArticleCharset").text.trim,
      (boardElement \ "Fan2Jian" text) != "0",
      (boardElement \ "Cookie" text),
      (boardElement \ "Javascript" text),
      (boardElement \ "UseProxy" text) != "0",
      (boardElement \ "ExtratorType" text),
      (boardElement \ "ExtratorFile" text),
      (boardElement \ "InfoCheck" text),
      ArticleFilter(
        (boardElement \ "ArticleFilter" \ "UrlRule").text.trim,
        (boardElement \ "ArticleFilter" \ "NUrlRule").text.trim,
        toInt((boardElement \ "ArticleFilter" \ "MinAnchorLength").text.trim),
        toInt((boardElement \ "ArticleFilter" \ "MinTextLength").text.trim), {
          val node = boardElement \ "ArticleFilter" \ "StringMatcher"
          StringMatcher((node \ "Enabled" text) != "0",
            (node \ "KeywordsFile").text.trim,
            toInt((node \ "MinDist").text.trim),
            toInt((node \ "Field").text.trim),
            toInt((node \ "HitHandler").text.trim),
            toInt((node \ "MissHandler").text.trim)
          )
        }, {
          val node = boardElement \ "ArticleFilter" \ "StopFilter"
          StopFilter(
            (node \ "Enabled" text) != "0",
            (node \ "StopwordsFile" text),
            toInt((node \ "Field").text.trim)
          )
        }
      ), {
        val node = boardElement \ "Image"
        ImageRule(
          (node \ "Flag" text) != "0",
          (node \ "InText" text) != "0",
          (node \ "ImgUrlRule" text)
        )
      },

      toInt((boardElement \ "GatherDepth").text.trim),
      toInt((boardElement \ "UpdateMinutes").text.trim),
      toInt((boardElement \ "ReqTimeInterval").text.trim),
      (boardElement \ "NextPageUrlRule").text.trim,
      toInt((boardElement \ "MaxNextPageCount").text.trim),
      toInt((boardElement \ "GatherDayNum").text.trim)
    )
  }

  def toInt(value: String) = NumberUtils.toInt(value, 0)

  def get(id: String): Option[Board] = BoardDb.get(id toString)
}
