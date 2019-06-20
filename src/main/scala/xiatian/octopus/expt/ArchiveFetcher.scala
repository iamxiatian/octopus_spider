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

  def main(args: Array[String]): Unit = {
//    fetchAll()
//    toXML()
  }
}
