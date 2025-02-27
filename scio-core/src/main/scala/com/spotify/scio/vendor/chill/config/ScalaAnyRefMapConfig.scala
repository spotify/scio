/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package com.spotify.scio.vendor.chill.config

object ScalaAnyRefMapConfig {
  def apply(m: Map[_ <: AnyRef, _ <: AnyRef]): ScalaAnyRefMapConfig =
    new ScalaAnyRefMapConfig(m.map { case (k, v) =>
      (k.asInstanceOf[AnyRef], v.asInstanceOf[AnyRef])
    })
  def empty: ScalaAnyRefMapConfig = new ScalaAnyRefMapConfig(Map.empty)
}

/**
 * A simple config backed by an immutable Map. Note that this replaces ScalaMapConfig because in
 * cascading non-string values are perfectly legal.
 */
class ScalaAnyRefMapConfig(in: Map[AnyRef, AnyRef]) extends Config {
  private var conf = in

  def toMap: Map[AnyRef, AnyRef] = conf

  override def get(k: String): String = conf.get(k).map(_.toString).getOrElse(null)
  override def set(k: String, v: String): Unit =
    Option(v) match {
      case Some(_) => conf += k -> v
      case None    => conf -= k
    }
}
