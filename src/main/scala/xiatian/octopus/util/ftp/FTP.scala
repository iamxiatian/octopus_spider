package xiatian.octopus.util.ftp

import java.io.{File, FileOutputStream, InputStream}

import org.apache.commons.net.ftp.FTP.BINARY_FILE_TYPE
import org.apache.commons.net.ftp._

import scala.io.Source.fromInputStream
import scala.util.Try

final class FTP(val client: FTPClient) {
  def login(username: String, password: String): Try[Boolean] = Try {
    client.login(username, password)
  }

  def connect(host: String, port: Int): Try[Unit] = Try {
    client.connect(host, port)
    client.enterLocalPassiveMode()
  }

  def connected: Boolean = client.isConnected

  def disconnect(): Unit = client.disconnect()

  def canConnect(host: String): Boolean = {
    client.connect(host)
    val connectionWasEstablished = connected
    client.disconnect()
    connectionWasEstablished
  }

  def listFiles(dir: Option[String]): Array[FTPFile] = dir match {
    case Some(d) => client.listFiles(d)
    case None => client.listFiles
  }

  def connectWithAuth(host: String,
                      port: Int = 21,
                      username: String = "anonymous",
                      password: String = ""): Try[Boolean] = {
    for {
      connection <- connect(host, port)
      login <- login(username, password)
    } yield login
  }

  def extractNames(f: Option[String] => Array[FTPFile]) =
    f(None).map(_.getName).toSeq

  def cd(path: String): Boolean =
    client.changeWorkingDirectory(path)

  def filesInCurrentDirectory: Seq[String] =
    extractNames(listFiles)

  def downloadFileStream(remote: String): InputStream = {
    client.setFileType(BINARY_FILE_TYPE)
    val stream = client.retrieveFileStream(remote)
    client.completePendingCommand() // make sure it actually completes!!
    stream
  }

  def downloadFile(remote: String, local: File): Boolean = {
    val os = new FileOutputStream(local)
    client.setFileType(BINARY_FILE_TYPE)
    val result = client.retrieveFile(remote, os)
    os.close()

    result
  }

  def streamAsString(stream: InputStream): String =
    fromInputStream(stream).mkString

  def uploadFile(remote: String, input: InputStream): Boolean = client.storeFile(remote, input)

}