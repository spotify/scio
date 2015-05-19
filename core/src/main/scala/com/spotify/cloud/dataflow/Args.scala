package com.spotify.cloud.dataflow

import scala.util.control.NonFatal

object Args {

  def apply(args: Array[String]): Args = {
    val (properties, booleans) = args.map { arg =>
      if (!arg.startsWith("--")) throw new IllegalArgumentException(s"Argument '$arg' does not begin with '--'")
      arg.substring(2)
    }.partition(_.contains("="))

    val propertyMap = properties.map { s =>
      val Array(k, v) = s.split("=", 2)
      (k, v.split(","))
    }.groupBy(_._1).mapValues(_.flatMap(_._2).toList).toMap
    val booleanMap = booleans.map((_, List("true"))).toMap

    propertyMap.keySet.intersect(booleanMap.keySet).foreach { arg =>
      throw new IllegalArgumentException(s"Conflicting boolean and property '$arg'")
    }

    // Workaround to ensure Map is serializable
    val m = (propertyMap ++ booleanMap).map(identity)

    new Args(m)
  }

}

class Args(val m: Map[String, List[String]]) {

  def apply(key: String): String = required(key)

  def getOrElse(key: String, default: String): String = optional(key).getOrElse(default)

  def list(key: String): List[String] = m.getOrElse(key, List())

  def optional(key: String): Option[String] = list(key) match {
    case List() => None
    case List(v) => Some(v)
    case _ => throw new IllegalArgumentException(s"Multiple values for property '$key'")
  }

  def required(key: String): String = list(key) match {
    case List() => throw new IllegalArgumentException(s"Missing value for property '$key'")
    case List(v) => v
    case _ => throw new IllegalArgumentException(s"Multiple values for property '$key'")
  }

  def int(key: String, default: Int): Int = getOrElse(key, default, _.toInt)

  def int(key: String): Int = get(key, _.toInt)

  def long(key: String, default: Long): Long = getOrElse(key, default, _.toLong)

  def long(key: String): Long = get(key, _.toLong)

  def float(key: String, default: Float): Float = getOrElse(key, default, _.toFloat)

  def float(key: String): Float = get(key, _.toFloat)

  def double(key: String, default: Double): Double = getOrElse(key, default, _.toDouble)

  def double(key: String): Double = get(key, _.toDouble)

  private def getOrElse[T](key: String, default: T, f: String => T): T = {
    optional(key).map(value => try f(value) catch {
      case NonFatal(_) => throw new IllegalArgumentException(s"Invalid value '$value' for '$key'")
    }).getOrElse(default)
  }

  private def get[T](key: String, f: String => T): T = {
    val value = required(key)
    try f(value) catch {
      case NonFatal(_) => throw new IllegalArgumentException(s"Invalid value '$value' for '$key'")
    }
  }

}
