package xiatian.octopus.util

import scala.xml.{Elem, XML}

/**
  * 行政区划信息, 可以通过行政区划的代号，或者名称获取对应的行政区划；
  * 同时维持了行政区划之间的隶属关系。使用方式：
  *
  * ===列出所有的省份信息：===
  *
  * {{{
  * District.provinces.foreach{
  *   p => println(s"${p.code}\t${p.name}")
  * }
  * }}}
  *
  * === 获取一个省份的信息 ===
  * {{{
  * District.province("北京市")
  * }}}
  * 或者：
  * {{{
  * District.province("110000")
  * }}}
  */
object District {

  private def makeCounties(cityCode: String, cityElem: Elem): List[County] =
    (cityElem \ "County").map {
      case countyElem: Elem =>
        val code = (countyElem \ "@Code").text
        val name = (countyElem \ "@Text").text

        County(code, name, cityCode)
    }.toList


  private def makeCities(provinceCode: String, provinceElem: Elem): List[City] =
    (provinceElem \ "City").map {
      case cityElem: Elem =>
        val cityCode = (cityElem \ "@Code").text
        val cityName = (cityElem \ "@Text").text

        City(cityCode, cityName, provinceCode, makeCounties(cityCode, cityElem))
    }.toList

  val provinces: List[Province] = {
    val stream = getClass.getResourceAsStream("/district.xml")

    val doc = XML.load(stream)
    val provinceElements = doc \\ "Province"
    provinceElements.map {
      case e: Elem =>
        val provinceCode = (e \ "@Code").text
        val provinceName = (e \ "@Text").text
        Province(provinceCode, provinceName, "CN", makeCities(provinceCode, e))
    }.toList
  }

  /**
    * 记录行政区域的代号到具体行政区域的映射关系
    */
  val cacheByCode: Map[String, District] = provinces.map {
    p: Province =>
      p :: p.cities ::: p.cities.flatMap(_.counties)
  }.flatten.map {
    d => (d.code, d)
  }.toMap

  val cacheByName: Map[String, District] = cacheByCode.values.map {
    d => (d.name, d)
  }.toMap

  private def isCode(s: String) = s.forall(_.isDigit)

  /**
    * 通过名称或者代号获取省份
    *
    * @param codeOrName
    * @return
    */
  def province(codeOrName: String): Option[Province] =
    (if (isCode(codeOrName)) cacheByCode else cacheByName)
      .get(codeOrName)
      .filter(_.isInstanceOf[Province])
      .map(_.asInstanceOf[Province])

  def city(codeOrName: String): Option[City] =
    (if (isCode(codeOrName)) cacheByCode else cacheByName)
      .get(codeOrName)
      .filter(_.isInstanceOf[City])
      .map(_.asInstanceOf[City])

  def county(codeOrName: String): Option[County] =
    (if (isCode(codeOrName)) cacheByCode else cacheByName)
      .get(codeOrName)
      .filter(_.isInstanceOf[County])
      .map(_.asInstanceOf[County])

}

trait District {
  def code: String

  def name: String
}

/**
  * 省、自治区、直辖市
  */
case class Province(code: String,
                    name: String,
                    countryCode: String,
                    cities: List[City]) extends District

/**
  * 地级市
  */
case class City(code: String,
                name: String,
                provinceCode: String,
                counties: List[County]) extends District

/**
  * 县
  */
case class County(code: String,
                  name: String,
                  cityCode: String) extends District
