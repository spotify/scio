package com.spotify.scio.util

import java.lang.{Iterable => JIterable}
import java.util.{Map => JMap}

import scala.collection.JavaConverters._

private[scio] object JMapWrapper {
  def ofMultiMap[A, B](self: JMap[A, JIterable[B]]): Map[A, Iterable[B]] =
    new Map[A, Iterable[B]] {
      // make eager copies when necessary
      // scalastyle:off method.name
      override def +[B1 >: Iterable[B]](kv: (A, B1)): Map[A, B1] =
        self.asScala.mapValues(_.asScala).toMap + kv
      override def -(key: A): Map[A, Iterable[B]] =
        self.asScala.mapValues(_.asScala).toMap - key
      // scalastyle:on method.name

      // lazy transform underlying j.l.Map
      override def get(key: A): Option[Iterable[B]] = Option(self.get(key)).map(_.asScala)
      override def iterator: Iterator[(A, Iterable[B])] =
        self.asScala.iterator.map(kv => (kv._1, kv._2.asScala))
    }

  def of[K, V](self: JMap[K, V]): Map[K, V] =
    new Map[K, V] {
      // make eager copies when necessary
      // scalastyle:off method.name
      override def +[B1 >: V](kv: (K, B1)): Map[K, B1] = self.asScala.toMap + kv
      override def -(key: K): Map[K, V] = self.asScala.toMap - key
      // scalastyle:on method.name

      // lazy transform underlying j.l.Map
      override def get(key: K): Option[V] = Option(self.get(key))
      override def iterator: Iterator[(K, V)] = self.asScala.iterator
    }
}


