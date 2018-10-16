package xiatian.octopus.util

import javax.mail.internet.InternetAddress
import org.slf4j.LoggerFactory
import xiatian.octopus.common.MyConf
import xiatian.octopus.util.mail.{Envelope, Mailer, Multipart}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Success


/**
  * 根据配置文件中的邮件设置参数，发送邮件
  */
object SmtpMailer {
  val log = LoggerFactory.getLogger("SmtpMailer")

  val host = MyConf.getString("scheduler.mail.smtp.host")
  val port = MyConf.getInt("scheduler.mail.smtp.port")
  val user = MyConf.getString("scheduler.mail.smtp.user")
  val password = MyConf.getString("scheduler.mail.smtp.password")
  val auth = MyConf.getBoolean("scheduler.mail.smtp.auth")
  val startTtls = MyConf.getBoolean("scheduler.mail.smtp.startTtls")

  val receivers = MyConf.getString("scheduler.mail.receivers").split(";")
    .filter(_.contains("@"))
    .map(new InternetAddress(_))
    .toList

  val mailer = Mailer(host, port)
    .auth(auth)
    .as(user, password)
    .startTtls(startTtls)()

  def sendMail(subject: String, htmlBody: String): Unit =
    mailer(
      Envelope.from(
        //"cnxiatian" `@` "163.com"
        new InternetAddress(user)
      )
        .to(
          //"cnxiatian" `@` "163.com"
          receivers: _*
        )
        .subject(subject)
        .content(Multipart()
          //.attach(new java.io.File("tps.xls"))
          .html(htmlBody)))
      .onComplete {
        case Success(_) =>
          log.info(s"Email about $subject has been deliverd.")
        case _ =>
          log.error(s"Error when sending email $subject")
      }
}