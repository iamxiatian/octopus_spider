package xiatian.octopus.util

object UUID {
  def newId = java.util.UUID.randomUUID().toString.replaceAll("-", "")

}
