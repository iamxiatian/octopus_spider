package xiatian.octopus.util.ftp

import org.apache.commons.net.ftp.{FTPClient => ApacheFTPClient}

object FTPClient {
  def apply(): FTP = new FTP(new ApacheFTPClient)

  def main(args: Array[String]): Unit = {
    // Test
    val client = FTPClient() // create a new FTP client instance
    client.connectWithAuth("10.1.1.1", 21, "user", "password")
    client.downloadFile("/static/5e49.png", new java.io.File("/tmp/aaa.png"))
  }
}