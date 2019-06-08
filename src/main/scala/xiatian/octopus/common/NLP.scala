package xiatian.octopus.common

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.Segment
import com.hankcs.hanlp.seg.common.Term

import scala.collection.mutable.Seq

object NLP {

  import scala.collection.JavaConverters._

  HanLP.Config.CustomDictionaryPath = Array("./data/dictionary/custom/CustomDictionary.txt")

  println("CustomDictionary:" + HanLP.Config.CustomDictionaryPath(0))
  //println(CustomDictionary.reload())
  val hanlp: Segment = HanLP.newSegment().enableCustomDictionary(true)

  def segment(text: String): Seq[Term] = hanlp.seg(text).asScala

  def main(args: Array[String]): Unit = {
    segment("黄埔军校的声像档案不错。") foreach println
  }
}
