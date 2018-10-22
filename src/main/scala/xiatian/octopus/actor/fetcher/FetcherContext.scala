package xiatian.octopus.actor.fetcher

import org.zhinang.util.cache.LruCache
import xiatian.octopus.task.FetchTask

/**
  * Fetcher的上下文信息。由于要支持分布式处理，Fetcher和Master运行在不同的机器下，
  * 因此，需要把Fetcher需要的上下文信息，通过SyncContextActor同步到Fetcher，保存
  * 在FetcherContext中。
  */
object FetcherContext {
  private val cacheSize = 2000
  val tasks = new LruCache[String, FetchTask](cacheSize)

  def getTask(id: String): Option[FetchTask] = Option(tasks.get(id))

}
