package xiatian.octopus.task

import java.io.{ByteArrayOutputStream, DataOutputStream}

import xiatian.octopus.model.{ArticleFetchType, FetchLink, HubFetchType}

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
      FetchLink(url, None, None, 1, 0, HubFetchType, id)
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
    if (link.depth > maxDepth || link.`type` == ArticleFetchType)
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
                link.depth + 1, 0, ArticleFetchType, link.taskId, link.params))
            else None
          } else if (acceptUrlPatterns.exists(_.r.pattern.matcher(url).matches()) && link.depth < maxDepth) {
            Option(FetchLink(url, Option(link.url), Option(anchor),
              link.depth + 1, 0, HubFetchType, link.taskId, link.params))
          } else None
      }.toList
    }

  /**
    * 把任务转换成二进制字节类型, 开始包含了两个整数，用于标记任务的类型和数据版本
    *
    * @return
    */
  override def toBytes: Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val dos = new DataOutputStream(out)

    dos.writeInt(FetchTask.TASK_TYPE_SITE)
    dos.writeInt(1) //version

    dos.writeUTF(id)
    dos.writeUTF(name)
    dos.writeUTF(homepage)

    dos.writeInt(entryUrls.size)
    entryUrls.foreach(dos.writeUTF)

    dos.writeInt(articleUrlPatterns.size)
    articleUrlPatterns.foreach(dos.writeUTF)

    dos.writeInt(acceptUrlPatterns.size)
    acceptUrlPatterns.foreach(dos.writeUTF)


    dos.writeInt(denyUrlPatterns.size)
    denyUrlPatterns.foreach(dos.writeUTF)

    dos.writeLong(secondInterval)
    dos.writeInt(maxDepth)
    dos.writeInt(minAnchorLength)

    dos.close()
    out.close()
    out.toByteArray
  }
}
