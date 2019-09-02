package xiatian.octopus.common

/**
  * 对象在集群中的标记：运行在客户端(Fetcher)，还是服务器端(Master)
  */
object ClusterTag {

  trait Remote

  trait Client

  trait RemoteClient extends Remote with Client
}
