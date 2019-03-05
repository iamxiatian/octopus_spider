package xiatian.octopus.task.epaper

import xiatian.octopus.parse.ParsedData

/**
  * 电子报的文章
  *
  * @param url
  * @param title
  * @param author
  * @param pubDate
  * @param column
  * @param rank
  * @param content
  */
case class EPaperArticle(url: String,
                         title: String,
                         author: String,
                         pubDate: String, //yyyy-MM-dd格式的日期
                         column: String, //所在栏目
                         rank: Int, //在该栏目的排序序号
                         text: String,
                         html: String
                        ) extends ParsedData
