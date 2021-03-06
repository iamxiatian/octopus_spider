package xiatian.octopus.actor

import akka.actor.Actor
import akka.event.LoggingAdapter

/**
  * 带有日志处理和开始与结束提示信息的基类
  */
trait ActorWatching {
  this: Actor ⇒
  private var _log: LoggingAdapter = _

  override def preStart: Unit = {
    LOG.warning(s"==> Starting ${this.self.path.name}...")
  }

  def LOG: LoggingAdapter = {
    // only used in Actor, i.e. thread safe
    if (_log eq null)
      _log = akka.event.Logging(context.system, this)
    _log
  }

  override def postStop(): Unit = {
    LOG.warning(s"==> ACTOR: ${this.self.path.name} stopped.")
  }

}