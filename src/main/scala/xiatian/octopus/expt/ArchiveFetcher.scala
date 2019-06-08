package xiatian.octopus.expt

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object ArchiveFetcher {

  def fetchAll(): Unit = {
    Await.result(ArchiveConsultDb.dropSchema, Duration.Inf)
    Await.result(ArchiveConsultDb.createSchema, Duration.Inf)

    println("process Guizhou")
    GuizhouArchive.collect()

    println("process Jiangxi ... ")
    JiangxiArchive.collect()

    println("process Chengdu...")
    ChengduArchive.collect()

    println("process Shenzhen...")
    ShenzhenArchive.collect()

    println("process Xi'an ...")
    XianArchive.collect()

    println("Process Tianjin ...")
    TianjinArchive.getAllPageList foreach {
      pair =>
        TianjinArchive.parsePage(pair._1, pair._2)
    }

    println("process Sichuan ...")
    (1 to 43) foreach {
      page =>
        println(s"process page $page ... ")
        SichuanArchive.parsePage(page).foreach {
          article =>
            Await.result(ArchiveConsultDb.save(article), Duration.Inf)
        }
    }

    println("process shaaxi ...")
    ShaanxiArchive.getPageList foreach {
      pair =>
        ShaanxiArchive.parsePage(pair._1, pair._2)
    }

    println("process guangxi ...")
    GuangxiArchive.getPageList foreach {
      pair =>
        GuangxiArchive.parseList(pair._1, pair._2)
    }
    println("process zhejiang ...")
    ZhejiangArchive.startParse()
  }

  /**
    * 把数据库里面的档案交互数据转存为XML格式
    */
  def toXML() = {
    import scala.xml.XML

    val records = Await.result(ArchiveConsultDb.list, Duration.Inf)
    val items = records.map {
      record =>
        <record>
          <site>
            {record.site}
          </site>
          <url>
            {record.url}
          </url>
          <url_md5>
            {record.urlMd5}
          </url_md5>
          <code>
            {record.code}
          </code>
          <title>
            {record.title}
          </title>
          <person>
            {record.person}
          </person>
          <ask_time>
            {record.askTime}
          </ask_time>
          <ask_content>
            {record.askContent}
          </ask_content>
          <replier>
            {record.replier}
          </replier>
          <reply_time>
            {record.replyTime}
          </reply_time>
          <reply_content>
            {record.replyContent}
          </reply_content>
          <view_count>
            {record.viewCount}
          </view_count>
          <category>
            {record.category}
          </category>
        </record>
    }

    val doc = <records>
      {items}
    </records>

    XML.save("/home/xiatian/writing/archive-consulting/data/dataset.xml", doc, "UTF-8", true, null)
  }

  def main(args: Array[String]): Unit = {
//    fetchAll()
//    toXML()
  }
}
