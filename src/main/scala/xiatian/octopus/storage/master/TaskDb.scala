package xiatian.octopus.storage.master

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8

import xiatian.octopus.common.{ClusterTag, MyConf}
import xiatian.octopus.storage.ast.KeyCachedFastDb
import xiatian.octopus.task.FetchTask

/**
  * 抓取目标任务数据库，每一个任务拥有一个唯一的ID，在保存时，采用id作为主键，
  * 对应Task的属性，转换为JSON字符串，作为value，保存到RocksDB数据库中。
  */
object TaskDb extends
  KeyCachedFastDb(new File(MyConf.masterDbPath, "task").getCanonicalPath)
  with ClusterTag.Remote {

  def getIds(): Seq[String] = keys.map(new String(_, UTF_8)).toSeq

  def save(task: FetchTask): TaskDb.type = {
    write(task.id.getBytes(UTF_8), task.toBytes)
    this
  }

  def getById(taskId: String): Option[FetchTask] = {
    read(taskId.getBytes(UTF_8)).flatMap(bytes => FetchTask.readFrom(bytes))
  }


}
