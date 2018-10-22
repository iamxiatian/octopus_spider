package xiatian.octopus.task

import java.io.{ByteArrayOutputStream, DataInputStream, DataOutputStream}


case class ArticleRule(urlRule: String, //文章URL判定规则（不区分大小写）
                       notUrlRule: String, //文章URL排除规则
                       minAnchorLength: Int = 10, // 锚文本最小长度（字节数）
                       minTextLength: Int = 100, //正文最小长度（字节数）
                       matcher: StringMatcher,
                       stopFilter: StopFilter
                      ) {
  def toBytes(): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val dos = new DataOutputStream(out)

    writeBytes(dos)

    dos.close()
    out.close()

    out.toByteArray
  }

  def writeBytes(dos: DataOutputStream): Unit = {
    dos.writeUTF(urlRule)
    dos.writeUTF(if (notUrlRule == null) "" else notUrlRule)
    dos.writeInt(minAnchorLength)
    dos.writeInt(minTextLength)

    matcher.writeBytes(dos)
    stopFilter.writeBytes(dos)
  }
}

object ArticleRule {
  def readFrom(din: DataInputStream) =
    new ArticleRule(
      din.readUTF(),
      din.readUTF(),
      din.readInt(),
      din.readInt(),
      StringMatcher.readFrom(din),
      StopFilter.readFrom(din)
    )
}

/**
  *
  * @param enabled      是否进行关键词过滤（0-否；1-是；缺省为0；为0时以下几项无效）
  * @param keywordsFile 过滤关键词文件
  * @param minDist      最小词间距离阈值（缺省为0时此机制不起作用）
  * @param field        最小词间距离阈值（缺省为0时此机制不起作用）
  * @param hitHandler   匹配时的处理策略（缺省为0：0-入库（不需审核）；1-入库（需审核）；2-不入库）
  * @param missHandler  未匹配时的处理策略（缺省为0：0-入库（不需审核）；1-入库（需审核）；2-不入库）
  */
sealed case class StringMatcher(enabled: Boolean = false,
                                keywordsFile: String = "",
                                minDist: Int = 0,
                                field: Int = 0,
                                hitHandler: Int = 2,
                                missHandler: Int = 2) {
  def toBytes(): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val dos = new DataOutputStream(out)

    writeBytes(dos)

    out.toByteArray
  }

  def writeBytes(dos: DataOutputStream): Unit = {
    dos.writeBoolean(enabled)
    dos.writeUTF(if (keywordsFile == null) "" else keywordsFile)
    dos.writeInt(minDist)
    dos.writeInt(field)
    dos.writeInt(hitHandler)
    dos.writeInt(missHandler)
  }
}

object StringMatcher {
  def readFrom(din: DataInputStream) =
    new StringMatcher(
      din.readBoolean(),
      din.readUTF(),
      din.readInt(),
      din.readInt(),
      din.readInt(),
      din.readInt()
    )
}

/**
  *
  * @param enabled       是否进行停用词过滤（缺省为0）
  * @param stopwordsFile 停用词文件，一行一个词。若Field指定字段中包含该文件中的任何一个词，该文将抛弃
  * @param field         过滤哪个字段（缺省为1：0-所有字段；1-标题；2-正文）
  */
sealed case class StopFilter(enabled: Boolean = false,
                             stopwordsFile: String = "",
                             field: Int = 1) {
  def toBytes(): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val dos = new DataOutputStream(out)

    writeBytes(dos)

    dos.close()
    out.close()

    out.toByteArray
  }

  def writeBytes(dos: DataOutputStream): Unit = {
    dos.writeBoolean(enabled)
    dos.writeUTF(if (stopwordsFile == null) "" else stopwordsFile)
    dos.writeInt(field)
  }
}

object StopFilter {
  def readFrom(din: DataInputStream) =
    new StopFilter(
      din.readBoolean(),
      din.readUTF(),
      din.readInt()
    )
}