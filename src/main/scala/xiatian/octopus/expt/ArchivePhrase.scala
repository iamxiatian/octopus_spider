package xiatian.octopus.expt

import better.files.File

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * 生成HanLP使用的个性化词典
  */
object ArchivePhrase {
  def getCustomPhrases(): Set[String] = {
    File("./data/dictionary/custom/CustomDictionary.txt").lines
      .map {
        _.split(" ").head
      }.toSet
  }

  def main(args: Array[String]): Unit = {
    val candidates: Set[String] = File("/home/xiatian/writing/archive-consulting/phrase.txt")
      .lines
      .filter(!_.trim.startsWith("#")).map(_.replaceAll(" ", "")).toSet

    val phraseCount = mutable.Map.empty[String, Int]

    val records = Await.result(ArchiveConsultDb.list, Duration.Inf)
    records foreach {
      r =>
        candidates foreach {
          c =>
            if (r.title.contains(c)) {
              phraseCount(c) = phraseCount.getOrElse(c, 0) + 1
            }

            if (r.askContent.contains(c)) {
              phraseCount(c) = phraseCount.getOrElse(c, 0) + 1
            }

            if (r.replyContent.contains(c)) {
              phraseCount(c) = phraseCount.getOrElse(c, 0) + 1
            }
        }
    }

    val writer = File("./data/dictionary/custom/CustomDictionary.txt").newFileWriter()
    phraseCount foreach {
      case (w, cnt) =>
        writer.write(s"$w n $cnt\n")
    }
    writer.close()
  }


}
