package com.spotify.cloud.dataflow.util

private[util] object ClosureCleaner {
  def apply[T <: AnyRef](obj: T): T = {
    com.twitter.chill.ClosureCleaner(obj)
    obj
  }
}
