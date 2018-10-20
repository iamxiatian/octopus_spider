package xiatian.octopus.actor.fetcher

import scala.util.matching.Regex
import scala.xml.{Node, XML}

case class Rule(regex: Regex, container: String)

/**
  * 站点的属性信息，部分站点无法自动准确获取处理信息，通过该类进行定
  */
case class SiteAttribute(
                          domain: String,
                          encoding: Option[String],
                          findLinkByRegex: Boolean = false,
                          rules: Seq[Rule]
                        )

object SiteAttribute {
  val doc = XML.loadFile("conf/site-attributes.xml")
  val sites: Map[String, SiteAttribute] = (doc \\ "site").map {
    node =>
      val sa = SiteAttribute(
        attributeValue(node, "domain").get,
        attributeValue(node, "encoding"),
        attributeValue(node, "findLinkBy").getOrElse("").equalsIgnoreCase("regex"),
        readRules(node)
      )

      (sa.domain, sa)
  }.toMap

  /**
    * 该域名是否需要用正则表达式抽取子链接
    *
    * @param domain
    * @return
    */
  def findLinkByRegex(domain: String) =
    sites.get(domain.toLowerCase()) match {
      case None => false
      case Some(sa) => sa.findLinkByRegex
    }

  def attributes(domain: String): Option[SiteAttribute] =
    sites.get(domain.toLowerCase())

  def getContainer(domain: String, url: String): Option[String] =
    if (domain == null)
      None
    else
      sites.get(domain.toLowerCase()) match {
        case None => None
        case Some(sa) =>
          val rule = sa.rules.find(_.regex.pattern.matcher(url).matches())
          if (rule.isEmpty)
            None
          else
            Some(rule.get.container)
      }

  /**
    * 获取节点node下的name属性
    */
  private def attributeValue(node: Node, name: String): Option[String] =
    if ((node \ s"@${name}").isEmpty)
      None
    else
      Some((node \ s"@${name}").toString())

  private def readRules(siteNode: Node): Seq[Rule] =
    (siteNode \ "rule").map {
      node =>
        Rule((node \ "regex").text.r,
          (node \ "container").text
        )
    }

}
