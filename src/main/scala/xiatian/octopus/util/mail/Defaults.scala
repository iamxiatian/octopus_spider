package xiatian.octopus.util.mail

import java.util.Properties

import javax.mail.{Session => MailSession}

import scala.concurrent.ExecutionContext

object Defaults {
  val session = MailSession.getDefaultInstance(new Properties())

  implicit val executionContext = ExecutionContext.Implicits.global
}
