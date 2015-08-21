package com.spotify.scio.coders

import java.lang.{Iterable => JIterable}

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.google.common.collect.Lists
import com.twitter.chill.KSerializer

import scala.collection.JavaConverters._

private class JIterableWrapperSerializer[T] extends KSerializer[Iterable[T]] {

  override def write(kser: Kryo, out: Output, obj: Iterable[T]): Unit = {
    obj.foreach { t =>
      out.writeBoolean(true)
      kser.writeClassAndObject(out, t)
      out.flush()
    }
    out.writeBoolean(false)
    out.flush()
  }

  override def read(kser: Kryo, in: Input, cls: Class[Iterable[T]]): Iterable[T] = {
    val list = Lists.newArrayList[T]
    while (in.readBoolean()) {
      val item = kser.readClassAndObject(in).asInstanceOf[T]
      list.add(item)
    }
    list.asInstanceOf[JIterable[T]].asScala
  }

}
