package com.spotify.scio.util

import java.lang.{Iterable => JIterable}
import java.util.{List => JList}

import com.google.cloud.dataflow.sdk.values.KV

import scala.collection.JavaConverters._

private[scio] object TupleFunctions {

  def kvToTuple[K, V](kv: KV[K, V]): (K, V) = (kv.getKey, kv.getValue)

  def kvIterableToTuple[K, V](kv: KV[K, JIterable[V]]): (K, Iterable[V]) = (kv.getKey, kv.getValue.asScala)

  def kvListToTuple[K, V](kv: KV[K, JList[V]]): (K, Iterable[V]) =
    (kv.getKey, kv.getValue.asInstanceOf[JIterable[V]].asScala)

}
