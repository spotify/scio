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

package com.spotify.scio.values

import com.spotify.scio.util.CallSites

/** Trait for setting custom names on transforms. */
trait TransformNameable {
  private var nameProvider: TransformNameProvider = CallSiteNameProvider

  private[scio] def tfName: String = tfName(None)

  private[scio] def tfName(default: Option[String]): String = {
    val n = nameProvider match {
      case CallSiteNameProvider    => default.getOrElse(CallSiteNameProvider.name)
      case ConstNameProvider(name) => name
    }
    nameProvider = CallSiteNameProvider
    n
  }

  /** Set a custom name for the next transform to be applied. */
  def withName(name: String): this.type = {
    require(
      nameProvider.getClass != classOf[ConstNameProvider],
      s"withName() has already been used to set '$tfName' as the name for the next transform."
    )
    nameProvider = new ConstNameProvider(name)
    this
  }
}

sealed private trait TransformNameProvider {
  def name: String
}

private object CallSiteNameProvider extends TransformNameProvider {
  def name: String = CallSites.getCurrent
}

private case class ConstNameProvider(name: String) extends TransformNameProvider
