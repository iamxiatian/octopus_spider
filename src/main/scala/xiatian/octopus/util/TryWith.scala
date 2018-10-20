package xiatian.octopus.util

import java.io.Closeable

import scala.util.control.NonFatal
import scala.util.{Failure, Try}

/**
  * 自动关闭资源
  *
  * <pre>
  * TryWith(
  *scala.io.Source.fromFile(filename, encoding)
  * ) {
  * source =>
  *source.getLines()
  * ...
  * }
  * </pre>
  */
object TryWith {
  def apply[C <: Closeable, R](resource: => C)(f: C => R): Try[R] =
    Try(resource).flatMap(resourceInstance => {
      try {
        val returnValue = f(resourceInstance)
        Try(resourceInstance.close()).map(_ => returnValue)
      }
      catch {
        case NonFatal(exceptionInFunction) =>
          try {
            resourceInstance.close()
            Failure(exceptionInFunction)
          }
          catch {
            case NonFatal(exceptionInClose) =>
              exceptionInFunction.addSuppressed(exceptionInClose)
              Failure(exceptionInFunction)
          }
      }
    })
}