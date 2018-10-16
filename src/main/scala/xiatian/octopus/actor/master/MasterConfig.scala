package xiatian.octopus.actor.master

import xiatian.octopus.common.MyConf

/**
  * Master相关配置的特质
  *
  * @author Tian Xia
  *         Dec 05, 2016 13:22
  */
trait MasterConfig {

  //内存中每个桶内可以放的链接最大数量
  val maxBucketSize =
    MyConf.getInt("master.bucket.maxSize")

  //每次向桶内注入的链接数量
  val bucketLinkFillSize = MyConf.getInt("master.bucket.fillSize")

  //每个主机下的URL最多可以被几个Actor同时处理，该数值越大，
  // 对采集目标站点的压力越大，会提升IP被封的风险
  val maxActorsPerHost = 3

  //桶的数量: 推荐和爬虫的数量保持一致
  val numberOfBuckets = MyConf.getInt("fetcher.numOfFetchClientActors")

}
