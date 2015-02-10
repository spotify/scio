package com.spotify.cloud.dataflow.util

private[dataflow] object CallSites {

  private val ns = "com.spotify.cloud.dataflow."

  private val methodMap = Map("$plus$plus" -> "++")

  private def isExternalClass(c: String): Boolean =
    (!c.startsWith(ns) && !c.startsWith("scala.")) || c.startsWith(ns + "examples.")

  def getAppName: String = {
    Thread.currentThread().getStackTrace
      .drop(1)
      .find(e => isExternalClass(e.getClassName))
      .map(_.getClassName).getOrElse("unknown")
      .split("\\.").last.replaceAll("\\$$", "")
  }

  def getCurrent: String = {
    val stack = new Exception().getStackTrace.drop(1)
    val p = stack.indexWhere(e => isExternalClass(e.getClassName))

    val k = stack(p - 1).getMethodName
    val method = methodMap.getOrElse(k, k)
    val file = stack(p).getFileName
    val line = stack(p).getLineNumber
    s"$method@{$file:$line}"
  }

}
