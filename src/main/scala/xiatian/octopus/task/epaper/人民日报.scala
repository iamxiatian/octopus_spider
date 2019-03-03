package xiatian.octopus.task.epaper

import org.joda.time.DateTime
import xiatian.octopus.common.OctopusException
import xiatian.octopus.model.{FetchItem, FetchType}
import xiatian.octopus.parse.{ParseResult, Parser}

import scala.util.Try

object 人民日报 extends EPaperTask("人民日报电子报", "人民日报电子报") with Parser {
  override def entryItems: List[FetchItem] = {
    //默认返回最近一月的入口地址, 减去12小时，保证本天的第一次采集时间在12点之后

    (0 to 30).toList.map {
      days =>
        val d = DateTime.now().minusHours(12).minusDays(days)

        //http://paper.people.com.cn/rmrb/html/2019-03/01/nbs.D110000renmrb_01.htm
        val pattern = d.toString("yyyy-MM/dd")
        val url = s"http://paper.people.com.cn/rmrb/html/$pattern/" +
          s"nbs.D110000renmrb_01.htm"

        FetchItem(url, FetchType.EPaper.Column,
          Option("http://paper.people.com.cn/"),
          None,
          1,
          0,
          id
        )
    }
  }

  /**
    * 该任务对应的解析器, 利用该解析器可以对抓取条目进行解析，获取其中的内容
    *
    * @return
    */
  override def parser: Option[Parser] = Some(this)

  override def parse(item: FetchItem): Try[ParseResult] = Try {
    item.`type` match {
      case FetchType.EPaper.Column =>
      //TODO
        throw OctopusException(s"未识别的抓取条目: $item")
      case FetchType.EPaper.Article =>
      //TODO
        throw OctopusException(s"未识别的抓取条目: $item")
      case _ =>
        throw OctopusException(s"未识别的抓取条目: $item")
    }
  }

}
