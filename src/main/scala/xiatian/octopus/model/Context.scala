package xiatian.octopus.model

import xiatian.octopus.FastSerializable

/**
  * 爬虫任务的上下文信息，由Master传递到Client
  *
  * @version 当前上下文的版本，当版本发生变化后，客户端需要重新拉取服务器端的各类依赖
  *          信息，如解析规则。即服务器端通过该参数通知客户端有更新变化。
  */
case class Context(version: Int) extends FastSerializable {

}
