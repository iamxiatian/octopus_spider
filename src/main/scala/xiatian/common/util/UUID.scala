package xiatian.common.util

object UUID {
  def newId = java.util.UUID.randomUUID().toString.replaceAll("-", "")

}
