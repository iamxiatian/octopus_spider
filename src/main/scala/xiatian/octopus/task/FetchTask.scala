package xiatian.octopus.task

import java.io.{ByteArrayInputStream, DataInputStream}

import org.slf4j.LoggerFactory
import xiatian.octopus.actor.Context
import xiatian.octopus.common.OctopusException
import xiatian.octopus.model._

import scala.util.{Failure, Success, Try}


/**
  * 任务特质，各类任务具有的基本行为. 典型的抓取任务如针对某个网站的全站采集，
  * 针对某个关键词的定向采集。
  */
trait FetchTask {

  /** 任务 ID */
  def id: String

  /** 任务名称 */
  def name: String

  /** 该任务下可以处理的链接类型 */
  def supportedLinkTypes: Set[LinkType] = Set.empty

  /**
    * 是否接收该链接，作为本任务的一个抓取链接. 如果可以，返回link，否则返回None
    *
    * @param link
    */
  def filter(link: FetchLink): Option[FetchLink] = None

  /**
    * 返回间隔多少秒之后会再次抓取的秒数, 如果永远不再抓取，返回None
    *
    * @param link
    * @return
    */
  def nextFetchSeconds(link: FetchLink): Option[Long] = None

  /**
    * 该任务对应的入口链接
    *
    * @return
    */
  def entryLinks: List[FetchLink]


  /**
    * 根据url和锚文本，以及所在的页面链接，转换为FetchLink对象
    *
    * @param link
    * @param urlAnchorPairs
    * @return
    */
  def makeChildLinks(link: FetchLink, urlAnchorPairs: Map[String, String]): List[FetchLink]

  /**
    * 把任务转换成二进制字节类型, 开始包含了两个整数，用于标记任务的类型和数据版本
    *
    * @return
    */
  def toBytes: Array[Byte]
}

/**
  * 由文章网页Article和中心网页Hub构成的抓取任务
  */
private[task] trait ArticleHubTask extends FetchTask {
  def maxDepth: Int

  def secondInterval: Long

  /** 该任务下可以处理的链接类型 */
  override def supportedLinkTypes: Set[LinkType] = Set(ArticleLinkType, HubLinkType)

  /**
    * 是否接收该链接，作为本任务的一个抓取链接. 如果可以，返回link，否则返回None
    *
    * @param link
    */
  override def filter(link: FetchLink): Option[FetchLink] = link.`type` match {
    case ArticleLinkType =>
      if (link.depth > maxDepth + 1) None
      else Some(link)
    case HubLinkType =>
      if (link.depth > maxDepth) None else Some(link)
    case _ => None
  }

  /**
    * 返回间隔多少秒之后会再次抓取的秒数, 如果永远不再抓取，返回None
    *
    * @param link
    * @return
    */
  override def nextFetchSeconds(link: FetchLink): Option[Long] = link.`type` match {
    case ArticleLinkType => None //文章链接永不重复抓取
    case HubLinkType =>
      val seconds = secondInterval * Math.pow(2, link.retries) * Math.pow(2, link.depth - 1)
      Some(seconds.toLong)
  }
}


object FetchTask {
  private val LOG = LoggerFactory.getLogger(FetchTask.getClass)

  val TASK_TYPE_SITE = 1
  val TASK_TYPE_TOPIC = 2

  def apply(bytes: Array[Byte]): Option[FetchTask] = Try {
    val din = new DataInputStream(new ByteArrayInputStream(bytes))

    val taskType = din.readInt()
    val version = din.readInt()
    taskType match {
      case TASK_TYPE_SITE =>

      case _ =>
        throw new OctopusException(s"Unsupport fetch task $taskType")
    }

    val url = din.readUTF()
    din: DataInputStream
    ArticleSiteTask()

  } match {
    case Success(t) => Option(t)
    case Failure(e) =>
      LOG.error("error restore fetch task.", e)
      None
  }

  def context(taskId: String): Option[Context] = Option {
    //@TODO 修改此处
    //Context(FetchTask(""))
    null
  }

  def count(): Int = 0

  def get(link: FetchLink): Option[FetchTask] = get(link.taskId).orElse {
    LOG.warn(s"invalid task id ${link.taskId}, url=>${link.url}")
    None
  }

  def get(taskId: String): Option[FetchTask] = {
    taskId match {
      //case AmazonBookTask.id => Some(AmazonBookTask)
      case _ =>
        LOG.error(s"task $taskId does not exist.")
        None
    }
  }
}


