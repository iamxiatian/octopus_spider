package xiatian.octopus.task

import xiatian.octopus.model.{FetchLink, HubLink}

/**
  * 文章站点任务，通过文章报道提供信息的网站
  *
  * @param secondInterval 间隔多少秒才会二次抓取
  */
case class ArticleSiteTask(id: String,
                           name: String,
                           homepage: String,
                           entryUrls: List[String],
                           articleUrlPatterns: List[String],
                           acceptUrlPatterns: List[String],
                           denyUrlPatterns: List[String],
                           secondInterval: Long,
                           maxDepth: Int) extends ArticleHubTask {
  /**
    * 该任务对应的入口链接
    *
    * @return
    */
  override def entryLinks: List[FetchLink] = entryUrls.map {
    url =>
      FetchLink(url, None, None, 1, 0, HubLink, id)
  }
}
