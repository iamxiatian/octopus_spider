package xiatian.octopus.actor.master

import xiatian.octopus.model.FetchLink

/**
  * 桶选择器：根据URL的主机地址，选择对应的桶。
  */
sealed trait BucketPicker {
  type BucketIdx = Int

  def pick(link: FetchLink) = BucketController.buckets(pickIdx(link))

  /**
    * 根据传入的主机地址，选择某个桶，并返回该桶的编号
    */
  protected def pickIdx(link: FetchLink): BucketIdx

}

object SimplePicker extends BucketPicker {
  def pickIdx(link: FetchLink): BucketIdx = {
    link.getHost().hashCode.abs % BucketController.numberOfBuckets
  }
}

object AdvancedPicker extends BucketPicker {
  //TODO
  def pickIdx(link: FetchLink): BucketIdx = {
    link.getHost().hashCode.abs % BucketController.numberOfBuckets
  }

  //  val cache = new LruCache[String, BucketIdx](2000)
  //
  //  def pickIdx(link: FetchLink): BucketIdx = {
  //    val host = link.getHost()
  //    val defaultIdx = host.hashCode.abs % BucketController.numberOfBuckets
  //    val cachedIdx: Int = if (cache.containsKey(host)) cache.get(host) else -1
  //    val defaultBucket = BucketController.buckets(defaultIdx)
  //
  //    if (cachedIdx >= 0) {
  //      //先判断默认的桶是否空闲，如果空闲，则重新选择默认的桶
  //      if (defaultBucket.dataLessThan(0.5) && defaultBucket.navLessThan(0.5)) {
  //        cache.remove(host)
  //        defaultIdx
  //      } else {
  //        cachedIdx
  //      }
  //    } else if (link.isDataPage && defaultBucket.dataMoreThan(0.95)) {
  //      val bestIdx = BucketController.buckets.map(_.dataCount()).zipWithIndex.min._2
  //      cache.put(host, bestIdx)
  //      bestIdx
  //    } else if (!link.isDataPage && defaultBucket.navMoreThan(0.95)) {
  //      val bestIdx = BucketController.buckets.map(_.navCount()).zipWithIndex.min._2
  //      cache.put(host, bestIdx)
  //      bestIdx
  //    } else {
  //      defaultIdx
  //    }
  //  }
}