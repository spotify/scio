package com.spotify.scio.coders.instances

import java.lang.{Iterable => JIterable}
private[coders] object JavaCollectionWrappers {

  // private classes
  val JIterableWrapperClass: Class[_] =
    Class.forName("scala.collection.convert.JavaCollectionWrappers.JIterableWrapper")
  val JCollectionWrapperClass: Class[_] =
    Class.forName("scala.collection.convert.JavaCollectionWrappers.JCollectionWrapper")
  val JListWrapperClass: Class[_] =
    Class.forName("scala.collection.convert.JavaCollectionWrappers.JListWrapper")

  object JIterableWrapper {
    def unapply(arg: Any): Option[JIterable[_]] = arg match {
      case arg if arg.getClass == JListWrapperClass =>
        val underlying = JListWrapperClass
          .getField("underlying")
          .get(arg)
          .asInstanceOf[JIterable[_]]
        Some(underlying)
      case _ => None
    }
  }
}