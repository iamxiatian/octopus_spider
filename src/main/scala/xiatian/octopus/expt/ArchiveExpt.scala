package xiatian.octopus.expt

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import org.apache.commons.lang3.StringUtils

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * 档案咨询交互数据的分析
  */
object ArchiveExpt {
  def records = Await.result(ArchiveConsultDb.list, Duration.Inf)

  //将记录转换为单词序列的记录
  var askWords = mutable.Map.empty[String, Int]
  var replyWords = mutable.Map.empty[String, Int]

  def load() = records.foreach {
    a =>
      HanLP.segment(a.askContent).asScala
        .filter(accept)
        .map(_.word)
        .foreach {
          w =>
            val count = askWords.get(w).getOrElse(0) + 1
            askWords(w) = count
        }

      HanLP.segment(a.replyContent).asScala
        .filter(accept)
        .map(_.word)
        .foreach {
          w =>
            val count = replyWords.get(w).getOrElse(0) + 1
            replyWords(w) = count
        }
  }

  //输出最高频的100个词语及其词频
  def topAskWords = askWords.map {
    case (w, c) => (w, c)
  }.toList.sortBy(_._2)
    .reverse

  def topReplyWords = replyWords.map {
    case (w, c) => (w, c)
  }.toList.sortBy(_._2)
    .reverse

  def showTopWords(topN: Int) = {
    topAskWords.take(topN).zip(topReplyWords.take(topN)).foreach {
      case ((w1, c1), (w2, c2)) =>
        println(s"$w1\t$c1\t$w2\t$c2")
    }
  }

  def accept(term: Term): Boolean = {
    val word = term.word
    val nature: String = term.nature.toString
    word.length > 1 && !StringUtils.isNumericSpace(word) &&
      (nature.startsWith("n") || nature.startsWith("v"))
  }

  def build(args: Array[String]): Unit = {
    Await.result(ArchiveConsultDb.dropSchema, Duration.Inf)
    Await.result(ArchiveConsultDb.createSchema, Duration.Inf)

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

  }

  def main(args: Array[String]): Unit = {
    build(args)
  }
}
