package xiatian.octopus.util

import javax.mail.internet.InternetAddress

/** An agreeable email interface for scala. */
package object mail {

  implicit class addr(name: String) {
    def at = `@` _

    def `@`(domain: String): InternetAddress = new InternetAddress(s"$name@$domain")

    /** In case whole string is email address already */
    def addr = new InternetAddress(name)
  }

}
