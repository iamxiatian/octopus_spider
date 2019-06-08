package xiatian.octopus.expt

import java.nio.charset.StandardCharsets
import java.util.Date

import better.files.File
import com.hankcs.hanlp.seg.common.Term
import it.uniroma1.dis.wsngroup.gexf4j.core.Node
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.data.AttributeListImpl
import org.apache.commons.lang3.StringUtils
import org.joda.time.DateTime
import xiatian.octopus.common.NLP

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * 档案咨询交互数据的分析
  */
object ArchiveExpt {

  def records = Await.result(ArchiveConsultDb.list, Duration.Inf)

  val stopWords = Set("谢谢", "请问", "老师", "可以", "时候", "问题", "情况", "没有", "进行",
    "有关", "深圳档案信息网", "浙江", "杭州市", "深圳档案信息网", "天津档案", "感谢", "关注", "参加",
    "韩局", "知道", "咨询", "相关", "浙江省", "是否", "能否", "想问", "回复", "应该", "需要")

  //将记录转换为单词序列的记录
  var askWords = mutable.Map.empty[String, Int]
  var replyWords = mutable.Map.empty[String, Int]

  def load() = records.foreach {
    a =>
      //HanLP.cus
      NLP.segment(a.title + " " + a.askContent)
        .filter(accept)
        .map(_.word)
        .foreach {
          w =>
            val count = askWords.get(w).getOrElse(0) + 1
            askWords(w) = count
        }

      NLP.segment(a.replyContent)
        .filter(accept)
        .map(_.word)
        .foreach {
          w =>
            val count = replyWords.get(w).getOrElse(0) + 1
            replyWords(w) = count
        }
  }

  def saveWords() = {
    //保存所有的提问里面的词语
    topAskWords.foreach {
      case (w, askCount) =>
        val replyCount = replyWords.getOrElse(w, 0)
        Await.result(WordRepo.save(w, askCount, replyCount), Duration.Inf)
    }

    //保存回复里面的词语
    topReplyWords
      .filter(pair => !askWords.contains(pair._1))
      .foreach {
        case (w, replyCount) =>
          Await.result(WordRepo.save(w, 0, replyCount), Duration.Inf)
      }
  }

  //输出最高频的100个词语及其词频
  def topAskWords = askWords.map {
    case (w, c) => (w, c)
  }.toList.sortBy(_._2)
    .reverse.take(2000)

  def topReplyWords = replyWords.map {
    case (w, c) => (w, c)
  }.toList.sortBy(_._2)
    .reverse.take(2000)

  def showTopWords(topN: Int) = {
    topAskWords.take(topN).zip(topReplyWords.take(topN)).foreach {
      case ((w1, c1), (w2, c2)) =>
        println(s"$w1\t$c1\t$w2\t$c2")
    }
  }

  def acceptBigrams(acceptWords: Set[String], text: String): List[String] = {
    val words = NLP.segment(text)
      .filter(accept)
      .map(_.word)
      .toList

    val bigrams: List[String] = words.zipWithIndex
      .filter(pair => acceptWords.contains(pair._1))
      .flatMap {
        case (w, idx) =>
          //从idx向后数若干个，如果有符合要求的，则输出该对
          (idx until Math.min(words.length, idx + 6)).find {
            i =>
              val candidate = words(i)
              w != candidate && acceptWords.contains(candidate)
          }.map {
            i =>
              val nextWord = words(i)
              s"${w}-${nextWord}"
          }
      }

    bigrams
  }

  def saveBiWords() = {
    val keepWords = (topAskWords.take(500) ::: topReplyWords.take(500))
      .map(_._1)
      .toSet
    var bigramCache = mutable.Map.empty[String, Int]

    records.foreach {
      a =>
        //        val words1 = HanLP.segment(a.title + " " + a.askContent).asScala
        //          .filter(accept)
        //          .map(_.word)
        //          .filter(keepWords.contains(_))
        //          .toList
        //
        //        val words2 = HanLP.segment(a.replyContent).asScala
        //          .filter(accept)
        //          .map(_.word)
        //          .filter(keepWords.contains(_))
        //          .toList
        val pairs = acceptBigrams(keepWords, a.title) ::: acceptBigrams(keepWords,
          a.askContent) ::: acceptBigrams(keepWords, a.replyContent)

        pairs.foreach {
          bigram =>
            bigramCache(bigram) = bigramCache.getOrElse(bigram, 0) + 1
        }
    }

    //保存前5000个，存入数据库
    bigramCache.toList
      .sortWith((l, r) => l._2 > r._2)
      .take(5000)
      .foreach {
        case (bigram, cnt) =>
          Await.result(BiWordRepo.save(bigram, cnt), Duration.Inf)
      }
  }


  def savePhrases(): Unit = {
    load()

    val keepWords = (topAskWords.take(1000) ::: topReplyWords.take(1000))
      .map(_._1)
      .toSet
    var bigramCache = mutable.Map.empty[String, Int]

    records.foreach {
      a =>
        val text = s"${a.title}\n${a.askContent}\n${a.replyContent}"
        val words = NLP.segment(text)
          .map(_.word)
          .toList

        words.zipWithIndex
          .foreach {
            case (w, idx) =>
              val p = (idx until Math.min(words.length, idx + 3))
                .takeWhile(i => keepWords.contains(words(i))).toList

              if (p.size > 1) {
                val key = p.map(words(_)).mkString(" ")
                bigramCache(key) = bigramCache.getOrElse(key, 0) + 1
              }
          }
    }

    //保存前5000个
    val phrases = bigramCache.toList
      .sortWith((l, r) => l._2 > r._2)
      .take(5000)
      .filter {
        case (p, c) =>
          val minWordCount = p.split(" ").map {
            w =>
              replyWords.getOrElse(w, 0) + askWords.getOrElse(w, 0)
          }.min

          (c * 12) > minWordCount
      }

    File("./phrase.txt").writeText(phrases.map(_._1).mkString("\n"))
  }


  def accept(term: Term): Boolean = {
    val word = term.word
    val nature: String = term.nature.toString

    val allChar = word.toLowerCase.forall {
      ch =>
        ch == '&' || ch == '@' || ch == '~' || ch == '_' || ch == '>' || ch == '<' ||
          ch == '-' || (ch >= 'a' && ch <= 'z')
    }

    !stopWords.contains(word) &&
      !allChar &&
      word.length > 1 &&
      !StringUtils.isNumericSpace(word) &&
      (nature.startsWith("n") || nature.startsWith("v"))
  }

  def countAnalysis() = {
    val records = Await.result(ArchiveConsultDb.list, Duration.Inf)
      .filter(r => r.replyTime.getTime > r.askTime.getTime)


    //每个站点所拥有的所有交互数据数量
    val siteCounts = mutable.Map.empty[String, Int]

    // year -> (site, count)
    val countByReply = mutable.Map.empty[String, mutable.Map[String, Int]]
    records foreach {
      r =>
        val year = new DateTime(r.askTime.getTime).toString("yyyy")
        val site = r.site

        siteCounts(site) = siteCounts.getOrElse(site, 0) + 1

        countByReply(year) = {
          val sites = countByReply.getOrElse(year, mutable.Map.empty)
          val cnt = sites.getOrElse(site, 0) + 1
          sites(site) = cnt
          sites
        }
    }

    println("根据回复时间按年统计结果(归一化)")
    countByReply.map {
      case (year, sites) =>
        (year,
          sites.map {
            case (site, cnt) =>
              cnt * 100.0 / siteCounts(site)
          }.sum)
    }.toList.sortBy(_._1).foreach {
      case (year, cnt) =>
        println(s"($year, $cnt)")
    }
  }

  def countAnalysis2() = {
    val records = Await.result(ArchiveConsultDb.listWithoutSite("Zhejiang"), Duration.Inf)
    val countByAsk = mutable.Map.empty[String, Int]
    records foreach {
      r =>
        val year = new DateTime(r.askTime.getTime).toString("MM")
        countByAsk(year) = countByAsk.getOrElse(year, 0) + 1
    }

    println("根据提问时间按年统计结果")
    //print counts by year
    countByAsk.toList.sortBy(_._1).foreach {
      case (year, cnt) =>
        println(s"($year, $cnt)")
    }

    val countByReply = mutable.Map.empty[String, Int]
    records foreach {
      r =>
        val year = new DateTime(r.replyTime.getTime).toString("MM")
        countByReply(year) = countByReply.getOrElse(year, 0) + 1
    }

    println("根据回复时间按年统计结果")
    //println("Year Ask Reply")
    //print counts by year
    countByReply.toList.sortBy(_._1).foreach {
      case (year, cnt) =>
        println(s"($year, $cnt)")
    }
  }


  def replyAnalysis() = {
    //过滤掉错误的数据（回复时间早于提问时间）
    val records = Await.result(ArchiveConsultDb.list, Duration.Inf)
      .filter(r => r.replyTime.getTime > r.askTime.getTime)

    val recordsByYear = mutable.Map.empty[String, mutable.ListBuffer[ArchiveConsult]]

    records foreach {
      r =>
        val year = new DateTime(r.askTime.getTime).toString("yyyy")
        val rs = recordsByYear.getOrElse(year, mutable.ListBuffer.empty)
        rs.append(r)
        recordsByYear.put(year, rs)
    }

    recordsByYear.map {
      case (year, rs) =>
        val total = rs.map {
          r =>
            val days = (r.replyTime.getTime - r.askTime.getTime) / 86400000.0
            if (days < 0) 0 else days
        }.sum

        (year, total / rs.size)
    }.toList.sortBy(_._1).foreach {
      case (year, days) =>
        println(s"($year, $days)")
    }

    //    val hours = records.map {
    //      r =>
    //        val millis = r.replyTime.getTime - r.askTime.getTime
    //        if (millis == 0) {
    //          4
    //        } else {
    //          millis / 1000 / 60 / 60
    //        }
    //    }

    val days = records.map {
      r =>
        val days = (r.replyTime.getTime - r.askTime.getTime) / 86400000.0
        (r.site, r.url, days)
    }.sortWith((l, r) => l._3 > r._3)

    println("Top:\n_______________")
    days.take(1).foreach(println)

    println("Bottom:\n_______________")
    days.reverse.take(1) foreach (println)

    val days2 = days.reverse

    val p25 = days2.drop((days.size * 0.25).toInt).head._3.ceil
    val p50 = days2.drop((days.size * 0.5).toInt).head._3.ceil
    val p75 = days2.drop((days.size * 0.75).toInt).head._3.ceil

    val avg = days.map(_._3).sum / days.size

    println(s"p25: $p25")
    println(s"p50: $p50")
    println(s"p75: $p75")
    println(s"avg: $avg")

    //各个时间段的统计
    println(s"1--7: ${days.filter(x => x._3 <= 7).size}")
    println(s"8--15: ${days.filter(x => x._3 > 7 && x._3 <= 15).size}")
    println(s"16--30: ${days.filter(x => x._3 > 15 && x._3 <= 30).size}")
    println(s"31--90: ${days.filter(x => x._3 > 30 && x._3 <= 90).size}")
    println(s"91--180: ${days.filter(x => x._3 > 90 && x._3 <= 180).size}")
    println(s"181--365: ${days.filter(x => x._3 > 180 && x._3 <= 365).size}")
    println(s"365--: ${days.filter(x => x._3 > 365).size}")

    println("按照年度反馈所用天数分布情况")
    recordsByYear.map {
      case (year, rs) =>
        val total = rs.map {
          r =>
            val days = (r.replyTime.getTime - r.askTime.getTime) / 86400000.0
            if (days < 0) 0 else days
        }.sum

        (year, total / rs.size)
    }.toList.sortBy(_._1)
      .foreach {
        case (year, avg) =>
          println(s"($year, $avg)")
      }

    //按照回复需要的天数的分布情况
    println("按照回复需要的天数的分布情况")
    val dayDist = mutable.Map.empty[Int, Int] //回复天数的分布情况, key为天数，值为数量
    days foreach {
      case (site, url, day) =>
        val key = day.ceil.toInt
        dayDist(key) = dayDist.getOrElse(key, 0) + 1
    }

    var accumlator = 0
    dayDist.toList.sortBy(_._1).map {
      case (d, cnt) =>
        accumlator += cnt
        (d, accumlator * 1.0 / days.size)
    }.foreach {
      case (d, cnt) =>
        println(s"($d, $cnt)")
    }
  }

  def contentAnalysis(): Unit = {
    load()

    println("save words....")
    Await.result(WordRepo.dropSchema, Duration.Inf)
    Await.result(WordRepo.createSchema, Duration.Inf)
    saveWords()

    println("save biwords...")
    Await.result(BiWordRepo.dropSchema, Duration.Inf)
    Await.result(BiWordRepo.createSchema, Duration.Inf)
    saveBiWords()

    println("DONE")
  }

  def toDot(): Unit = {
    //val (graph, arc) = ("digraph", " -> ")   //有向图
    val (graph, arc) = ("graph", " -- ") //无向图

    val candidateWords = WordRepo.topAskWords(200).map(_.name).toSet
    val bigramsCandidate: Seq[BiWord] = Await.result(BiWordRepo.list(3000), Duration.Inf)
      .filter {
        b =>
          val Array(first, second) = b.bigram.split("-")
          candidateWords.contains(first) || candidateWords.contains(second)
      }

    println(bigramsCandidate.size)

    val phrases = ArchivePhrase.getCustomPhrases()
    val bigrams = bigramsCandidate.zipWithIndex.filter {
      case (b, idx) =>
        val Array(first, second) = b.bigram.split("-")
        (idx < 200 || phrases.contains(first) || phrases.contains(second)) &&
          !(b.bigram.startsWith("档案-") || b.bigram.endsWith("-档案")) &&
          !(b.bigram.startsWith("档案馆-") || b.bigram.endsWith("-档案馆"))
    } map (_._1)

    //根据选出来的bigram，记录每个节点的出现数量

    val nodeScores = mutable.Map.empty[String, Int]

    bigrams.foreach {
      b =>
        val Array(first, second) = b.bigram.split("-")
        nodeScores(first) = nodeScores.getOrElse(first, 0) + b.amount
        nodeScores(second) = nodeScores.getOrElse(second, 0) + b.amount
    }

    //把节点大小缩放到10~100之间
    val minScore = nodeScores.values.min
    val maxScore = nodeScores.values.max

    println(s"$minScore, $maxScore")
    val nodeText = nodeScores.map {
      case (n, score) =>
        //val w = (Math.log(score / minScore)*5/Math.log(2)).toInt + 10
        val w = ((score - minScore) * 1.0 / (maxScore - minScore) * 90 + 10).toInt
        if (score > 1000)
          s"""$n [shape = "egg", color="blue", fontsize=$w, label = "$n($score)"];"""
        else
          s"""$n [shape = "ellipse",fontsize=$w, color = "black", label = "$n($score)"];"""
    }.mkString("\n")

    val lines = bigrams.map {
      b =>
        val edge = b.bigram.replace("-", arc)
        s"$edge;"
    }.mkString("\n")

    //如果一个词语是另一个词语的一部分，则二者之间建立联系
    val nodes = bigrams.flatMap(b => b.bigram.split("-")).toSet.toList

    val buffer = mutable.ListBuffer.empty[String]

    //    (0 until nodes.length) foreach {
    //      idx =>
    //        val current = nodes(idx)
    //
    //        (idx + 1 until nodes.length) foreach {
    //          j =>
    //            val w = nodes(j)
    //            if (current.contains(w) || w.contains(current)) {
    //              buffer += s"${current}${arc}$w;"
    //            }
    //        }
    //    }

    val content =
      s"""
         |$graph g {
         |  fontname = "Microsoft Yahei"
         |  graph [ordering="out"];
         |  margin=0;
         |$nodeText
         |$lines
         |${buffer.mkString("\n")}
         |}
     """.stripMargin

    File("/home/xiatian/writing/archive-consulting/g.dot").writeText(content)
  }

  def toGexf(topKeywords: Int = 200): Unit = {
    import it.uniroma1.dis.wsngroup.gexf4j.core.EdgeType
    import it.uniroma1.dis.wsngroup.gexf4j.core.Gexf
    import it.uniroma1.dis.wsngroup.gexf4j.core.Mode
    import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeClass
    import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeType
    import it.uniroma1.dis.wsngroup.gexf4j.core.impl.GexfImpl

    val gexf = new GexfImpl

    gexf.getMetadata.setLastModified(new Date())
      .setCreator("Xia Tian")
      .setDescription("A Web network")

    gexf.setVisualization(true)

    val graph = gexf.getGraph
    graph.setDefaultEdgeType(EdgeType.DIRECTED).setMode(Mode.STATIC)

    val attrList = new AttributeListImpl(AttributeClass.NODE)
    graph.getAttributeLists.add(attrList)

    val attUrl = attrList.createAttribute("class", AttributeType.INTEGER, "Class")
    val attIndegree = attrList.createAttribute("pageranks", AttributeType.DOUBLE, "PageRank")


    //处理数据
    val candidateWords = WordRepo.topAskWords(topKeywords).map(_.name).toSet
    val bigramsCandidate: Seq[BiWord] = Await.result(BiWordRepo.list(3000), Duration.Inf)
      .filter {
        b =>
          val Array(first, second) = b.bigram.split("-")
          candidateWords.contains(first) || candidateWords.contains(second)
      }

    val phrases = ArchivePhrase.getCustomPhrases()
    val bigrams: Seq[BiWord] = bigramsCandidate.zipWithIndex.filter {
      case (b, idx) =>
        val Array(first, second) = b.bigram.split("-")
        (idx < 200 || phrases.contains(first) || phrases.contains(second)) &&
          !(b.bigram.startsWith("档案-") || b.bigram.endsWith("-档案")) &&
          !(b.bigram.startsWith("档案馆-") || b.bigram.endsWith("-档案馆"))
    } map (_._1)

    //根据选出来的bigram，记录每个节点的出现数量
    val nodeScores = mutable.Map.empty[String, Int]
    bigrams.foreach {
      b =>
        val Array(first, second) = b.bigram.split("-")
        nodeScores(first) = nodeScores.getOrElse(first, 0) + b.amount
        nodeScores(second) = nodeScores.getOrElse(second, 0) + b.amount
    }


    //把节点大小缩放到10~100之间
    val minScore = nodeScores.values.min
    val maxScore = nodeScores.values.max

    //创建gexf node
    nodeScores foreach {
      case (n, score) =>
        //val w = (Math.log(score / minScore)*5/Math.log(2)).toInt + 10
        val w = ((score - minScore) * 1.0 / (maxScore - minScore) * 90 + 10).toInt
        graph.createNode(n).setLabel(n).setSize(w)
    }

    //create edge
    bigrams foreach {
      b =>
        val Array(first, second) = b.bigram.split("-")
        graph.getNode(first).connectTo(graph.getNode(second))
    }

    import it.uniroma1.dis.wsngroup.gexf4j.core.impl.StaxGraphWriter
    val graphWriter = new StaxGraphWriter
    val out = File("/home/xiatian/writing/archive-consulting/g.gexf").newOutputStream()
    graphWriter.writeToStream(gexf, out, "UTF-8")
    out.close()
  }

  def main(args: Array[String]): Unit = {
    // replyAnalysis()

    //保存当前的停用词
    //    File("/home/xiatian/writing/archive-consulting/stopword.txt")
    //      .writeText(stopWords.mkString("\n"))

    //    contentAnalysis()
    //    toDot()
    toGexf()
  }
}
