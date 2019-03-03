package xiatian.octopus.parse

import xiatian.octopus.model.FetchItem

/**
  * 解析结果, 包括解析出的子抓取条目和解析出来的数据
  */
case class ParseResult(children: List[FetchItem], data: Option[ParsedData])

/**
  * 代表从FetchItem中解析出的数据，例如一篇新闻报道
  */
trait ParsedData