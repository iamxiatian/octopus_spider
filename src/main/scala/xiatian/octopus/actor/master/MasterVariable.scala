package xiatian.octopus.actor.master

import xiatian.octopus.model.FetchItem

import scala.collection.mutable

/**
  * 采集服务控制器抓取的数据量记录
  *
  * @author Tian Xia
  *         Jun 07, 2017 18:15
  */
object MasterVariable {
  // 每个任务下抓取的网页数量, 每个任务中又按照链接的类型保存了链接类型ID到抓取网页的数量
  // 之间的映射关系。
  val taskFetchedStats = new mutable.HashMap[String, mutable.HashMap[Int, Int]]

  //记录所有任务各不同链接类型到抓取数量之间的映射关系
  val fetchedStats = new mutable.HashMap[Int, Int]

  val connectedFetchers = new mutable.HashSet[String]()

  //统计信息
  var failureLinkCount = 0

  //最近一次报告信息
  var lastReportTime = 0L
  var lastReportMsg = """{"status": "ERROR", "data":"wait to initialize."}"""

  /**
    * 统计抓取的链接信息
    *
    * @param link
    */
  def countFetchItem(link: FetchItem): Unit = {
    val taskId = link.taskId.toString
    if (!taskFetchedStats.contains(taskId)) {
      taskFetchedStats.put(taskId, new mutable.HashMap[Int, Int]())
    }

    // 记录当前任务下的抓取数据
    val stats = taskFetchedStats.getOrElse(taskId, new mutable.HashMap[Int, Int])
    stats.put(link.`type`.id, stats.getOrElse(link.`type`.id, 0) + 1)

    //记录整体的抓取数据
    fetchedStats.put(link.`type`.id, stats.getOrElse(link.`type`.id, 0) + 1)

    //持久化
    UrlManager.countSuccess(link)
  }
}
