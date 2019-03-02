package xiatian.octopus.common

class OctopusException(message: String, cause: Throwable)
  extends Exception(message, cause) {

  def this(message: String) = this(message, null)
}

object OctopusException {
  def apply(message: String, cause: Throwable) =
    new OctopusException(message, cause)

  def apply(message: String): OctopusException = new OctopusException(message)
}