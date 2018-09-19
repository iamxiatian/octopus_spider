package xiatian.spider.actor

import akka.actor.Actor
import akka.event.LoggingAdapter

/**
  * 带有日志处理和开始与结束提示信息的基类
  */
trait WatchActor {
  this: Actor ⇒
  private var _log: LoggingAdapter = _

  def LOG: LoggingAdapter = {
    // only used in Actor, i.e. thread safe
    if (_log eq null)
      _log = akka.event.Logging(context.system, this)
    _log
  }

  override def preStart: Unit = {
    LOG.warning(s"==> Starting ${this.self.path.name}...")
  }

  override def postStop(): Unit = {
    LOG.warning(s"==> ACTOR: ${this.self.path.name} stopped.")
  }

}