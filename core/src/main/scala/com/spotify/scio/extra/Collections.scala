package com.spotify.scio.extra

import com.google.common.collect.MinMaxPriorityQueue

import scala.collection.JavaConverters._

/** Utilities for Scala collection library. */
object Collections {

  private def topImpl[T](xs: Iterable[T], num: Int, ord: Ordering[T]): Iterable[T] = {
    require(num > 0, "num must be > 0")
    val size = math.min(num, xs.size)
    MinMaxPriorityQueue
      .orderedBy(ord.reverse)
      .expectedSize(size)
      .maximumSize(size)
      .create[T](xs.asJava)
      .asScala
  }

  private def topByKeyImpl[K, V](xs: Iterable[(K, V)], num: Int, ord: Ordering[V]): Map[K, Iterable[V]] = {
    require(num > 0, "num must be > 0")
    val size = math.min(num, xs.size)

    val m = scala.collection.mutable.Map[K, MinMaxPriorityQueue[V]]()
    xs.foreach { case (k, v) =>
      if (!m.contains(k)) {
        val pq = MinMaxPriorityQueue
          .orderedBy(ord.reverse)
          .expectedSize(size)
          .maximumSize(size)
          .create[V]()
        pq.add(v)
        m(k) = pq
      } else {
        m(k).add(v)
      }
    }
    m.mapValues(_.asScala).toMap
  }

  /** Enhance Array by adding a top method. */
  implicit class topArray[T](self: Array[T]) {
    def top(num: Int)(implicit ord: Ordering[T]): Iterable[T] = topImpl(self, num, ord)
  }

  /** Enhance Iterable by adding a top method. */
  implicit class topIterable[T](self: Iterable[T]) {
    def top(num: Int)(implicit ord: Ordering[T]): Iterable[T] = topImpl(self, num, ord)
  }

  /** Enhance Array by adding a topByKey method. */
  implicit class topByKeyArray[K, V](self: Array[(K, V)]) {
    def topByKey(num: Int)(implicit ord: Ordering[V]): Map[K, Iterable[V]] = topByKeyImpl(self, num, ord)
  }

  /** Enhance Iterable by adding a top method. */
  implicit class topByKeyIterable[K, V](self: Iterable[(K, V)]) {
    def topByKey(num: Int)(implicit ord: Ordering[V]): Map[K, Iterable[V]] = topByKeyImpl(self, num, ord)
  }

}
