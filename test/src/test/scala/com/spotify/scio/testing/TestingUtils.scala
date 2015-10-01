package com.spotify.scio.testing

import java.lang.{Iterable => JIterable}

import com.google.common.collect.Lists

import scala.collection.JavaConverters._

private[scio] object TestingUtils {

  // Value type Iterable[T] is wrapped from Java and fails equality check
  def iterable[T](elems: T*): Iterable[T] = Lists.newArrayList(elems: _*).asInstanceOf[JIterable[T]].asScala

}
