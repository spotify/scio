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

/** Common Finder to return the proper ValidationProvider */
object OverrideTypeProviderFinder {

  private val _instance = {
    // Load the class dynamically at compile time and runtime
    val classInstance = Try(Class.forName(System.getProperty("OVERRIDE_TYPE_PROVIDER", ""))
      .newInstance()
      .asInstanceOf[OverrideTypeProvider])
    classInstance match {
      case Success(value) => value
      case Failure(NonFatal(_)) => new DummyOverrideTypeProvider
    }
  }

  def getProvider: OverrideTypeProvider = _instance
}
