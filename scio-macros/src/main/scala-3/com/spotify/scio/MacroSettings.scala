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

import scala.quoted._
import scala.annotation.experimental

sealed private[scio] trait FeatureFlag
private[scio] object FeatureFlag {
  case object Enable extends FeatureFlag
  case object Disable extends FeatureFlag
}

private[scio] object MacroSettings extends MacroSettingsFlagGetter {

  /**
   * Makes it possible to configure fallback warnings by passing
   * "-Xmacro-settings:show-coder-fallback=true" as a Scalac option.
   */
  @experimental
  def showCoderFallback(quotes: Quotes): FeatureFlag =
    getFlag(quotes.reflect.CompilationInfo.XmacroSettings)("show-coder-fallback", FeatureFlag.Disable)

  /**
   * Aggressively cache Schema implicit definitions to speed up compilation. This might lead to the
   * incorrect implicit definition to be selected since implicits may escape their scope.
   */
  @experimental
  def cacheImplicitSchemas(quotes: Quotes): FeatureFlag =
    getFlag(quotes.reflect.CompilationInfo.XmacroSettings)("cache-implicit-schemas", FeatureFlag.Disable)
}