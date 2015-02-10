package com.spotify.cloud.dataflow.coders

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.google.cloud.dataflow.sdk.values.KV
import com.twitter.chill.KSerializer

private class KVSerializer[K, V] extends KSerializer[KV[K, V]] {

  override def write(kser: Kryo, out: Output, obj: KV[K, V]): Unit = {
    kser.writeClassAndObject(out, obj.getKey)
    kser.writeClassAndObject(out, obj.getValue)
  }

  override def read(kser: Kryo, in: Input, cls: Class[KV[K, V]]): KV[K, V] = {
    val k = kser.readClassAndObject(in).asInstanceOf[K]
    val v = kser.readClassAndObject(in).asInstanceOf[V]
    KV.of(k, v)
  }

}
