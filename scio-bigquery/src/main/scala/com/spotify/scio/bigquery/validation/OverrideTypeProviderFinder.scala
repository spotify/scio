/*
 * Copyright 2016 Spotify AB.
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

package com.spotify.scio.bigquery.validation

import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal
import org.slf4j.LoggerFactory

/** Common finder for the proper [[OverrideTypeProvider]]. */
object OverrideTypeProviderFinder {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val instance = {
    val default: OverrideTypeProvider = new DummyOverrideTypeProvider

    Option(sys.props("OVERRIDE_TYPE_PROVIDER"))
      .map { n =>
        Try(Class.forName(n)) match {
          case Success(value) =>
            value.newInstance()
              .asInstanceOf[OverrideTypeProvider]
          case Failure(ex @ NonFatal(_)) =>
            logger.warn(s"Class not found: $n")
            default
        }
      }.getOrElse(default)
  }

  def getProvider: OverrideTypeProvider = instance
}
