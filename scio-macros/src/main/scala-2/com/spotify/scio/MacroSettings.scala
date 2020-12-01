/*
 * Copyright 2019 Spotify AB.
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

import scala.reflect.macros.blackbox

sealed private[scio] trait FeatureFlag
private[scio] object FeatureFlag {
  case object Enable extends FeatureFlag
  case object Disable extends FeatureFlag
}

private[scio] object MacroSettings {
  private def getFlag(settings: List[String])(name: String, default: FeatureFlag): FeatureFlag = {
    val ss: Map[String, String] =
      settings
        .map(_.split("="))
        .map { case Array(k, v) =>
          (k.trim, v.trim)
        }
        .toMap

    ss.get(name)
      .map {
        case "true" =>
          FeatureFlag.Enable
        case "false" =>
          FeatureFlag.Disable
        case v =>
          throw new IllegalArgumentException(
            s"""Invalid value for setting -Xmacro-settings:$name,""" +
              s"""expected "true" or "false", got $v"""
          )
      }
      .getOrElse(default)
  }

  /**
   * Makes it possible to configure fallback warnings by passing
   * "-Xmacro-settings:show-coder-fallback=true" as a Scalac option.
   */
  def showCoderFallback(c: blackbox.Context): FeatureFlag =
    getFlag(c.settings)("show-coder-fallback", FeatureFlag.Disable)

  /**
   * Aggressively cache Schema implicit definitions to speed up compilation.
   * This might lead to the incorrect implicit definition to be selected
   * since implicits may escape their scope.
   */
  def cacheImplicitSchemas(c: blackbox.Context): FeatureFlag =
    getFlag(c.settings)("cache-implicit-schemas", FeatureFlag.Disable)
}
