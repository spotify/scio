/*
 * Copyright 2018 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio

import com.google.common.base.Splitter

import scala.collection.JavaConverters._
import scala.collection.breakOut
import scala.util.control.NonFatal

/**
 * A simple command line argument parser.
 *
 * Arguments can be either properties (`--key=value1,value2,...`) or booleans (`--test`).
 */
object Args {

  /** Parse arguments. */
  def apply(args: Array[String]): Args = {
    val (properties, booleans) = args.map { arg =>
      if (!arg.startsWith("--")) {
        throw new IllegalArgumentException(s"Argument '$arg' does not begin with '--'")
      }
      arg.substring(2)
    }.partition(_.contains("="))

    val propertyMap = properties.map { s =>
      val Array(k, v) = s.split("=", 2)
      (k, Splitter.onPattern(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)").split(v).asScala)
    }.groupBy(_._1).mapValues(_.flatMap(_._2).toList)
    val booleanMap: Map[String, List[String]] = booleans.map((_, List("true")))(breakOut)

    propertyMap.keySet.intersect(booleanMap.keySet).foreach { arg =>
      throw new IllegalArgumentException(s"Conflicting boolean and property '$arg'")
    }

    // Workaround to ensure Map is serializable
    val m = (propertyMap ++ booleanMap).map(identity)

    new Args(m)
  }

}

/** Encapsulate parsed commandline arguments. */
class Args private (private val m: Map[String, List[String]]) extends Serializable {

  /** All arguments as a map. */
  def asMap: Map[String, List[String]] = m

  def toString(start: String, sep: String, end: String): String = {
    m.keys.toArray.sorted.map { k =>
      val values = m(k) match {
        case v :: Nil => v.toString
        case vs => vs.mkString("[", ", ", "]")
      }
      s"--$k=$values"
    }.mkString(start, sep, end)
  }

  override def toString: String = toString("Args(", ", ", ")")

  override def equals(obj: Any): Boolean = obj match {
    case that: Args => this.m == that.m
    case _ => false
  }

  override def hashCode(): Int = m.hashCode()

  /** Shortcut for `required`. */
  def apply(key: String): String = required(key)

  /** Shortcut for `optional(key).getOrElse(default)`. */
  def getOrElse(key: String, default: String): String = optional(key).getOrElse(default)

  /** Get the list of values for a given key. */
  def list(key: String): List[String] = m.getOrElse(key, Nil)

  /** Get an `Option` if there is zero or one element for a given key. */
  def optional(key: String): Option[String] = list(key) match {
    case Nil => None
    case List(v) => Some(v)
    case _ => throw new IllegalArgumentException(s"Multiple values for property '$key'")
  }

  /** Get exactly one value for a given key. */
  def required(key: String): String = list(key) match {
    case Nil => throw new IllegalArgumentException(s"Missing value for property '$key'")
    case List(v) => v
    case _ => throw new IllegalArgumentException(s"Multiple values for property '$key'")
  }

  /** Get value as `Int` with a default. */
  def int(key: String, default: Int): Int = getOrElse(key, default, _.toInt)

  /** Get value as `Int`. */
  def int(key: String): Int = get(key, _.toInt)

  /** Get value as `Long` with a default. */
  def long(key: String, default: Long): Long = getOrElse(key, default, _.toLong)

  /** Get value as `Long`. */
  def long(key: String): Long = get(key, _.toLong)

  /** Get value as `Float` with a default. */
  def float(key: String, default: Float): Float = getOrElse(key, default, _.toFloat)

  /** Get value as `Float`. */
  def float(key: String): Float = get(key, _.toFloat)

  /** Get value as `Double` with a default. */
  def double(key: String, default: Double): Double = getOrElse(key, default, _.toDouble)

  /** Get value as `Double`. */
  def double(key: String): Double = get(key, _.toDouble)

  /** Get value as `Boolean` with a default. */
  def boolean(key: String, default: Boolean): Boolean = getOrElse(key, default, _.toBoolean)

  /** Get value as `Boolean`. */
  def boolean(key: String): Boolean = get(key, _.toBoolean)

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
