package xiatian.octopus.task

import xiatian.octopus.model.{ArticleLink, FetchLink, HubLink}

/**
  * 文章站点任务，通过文章报道提供信息的网站
  *
  * @param secondInterval  间隔多少秒才会二次抓取
  * @param minAnchorLength 锚文本最小长度（字符数）
  */
case class ArticleSiteTask(id: String,
                           name: String,
                           homepage: String,
                           entryUrls: List[String],
                           articleUrlPatterns: List[String],
                           acceptUrlPatterns: List[String],
                           denyUrlPatterns: List[String],
                           secondInterval: Long,
                           maxDepth: Int,
                           minAnchorLength: Int = 10) extends ArticleHubTask {
  /**
    * 该任务对应的入口链接
    *
    * @return
    */
  override def entryLinks: List[FetchLink] = entryUrls.map {
    url =>
      FetchLink(url, None, None, 1, 0, HubLink, id)
  }

  /**
    * 根据url和锚文本，以及所在的页面链接，转换为FetchLink对象
    *
    * @param link
    * @param urlAnchorPairs
    * @return
    */
  override def makeChildLinks(link: FetchLink,
                              urlAnchorPairs: Map[String, String]
                             ): List[FetchLink] =
    if (link.depth > maxDepth || link.`type` == ArticleLink)
      List.empty
    else {
      urlAnchorPairs.filter {
        case (url, anchor) =>
          //先去掉拒绝的链接
          !denyUrlPatterns.exists(_.r.pattern.matcher(url).matches())
      }.flatMap {
        case (url, anchor) =>
          if (articleUrlPatterns.exists(_.r.pattern.matcher(url).matches())) {
            //文章链接
            if (anchor.length >= minAnchorLength)
              Option(FetchLink(url, Option(link.url), Option(anchor),
                link.depth + 1, 0, ArticleLink, link.taskId, link.params))
            else None
          } else if (acceptUrlPatterns.exists(_.r.pattern.matcher(url).matches()) && link.depth < maxDepth) {
            Option(FetchLink(url, Option(link.url), Option(anchor),
              link.depth + 1, 0, HubLink, link.taskId, link.params))
          } else None
      }.toList
    }
}
