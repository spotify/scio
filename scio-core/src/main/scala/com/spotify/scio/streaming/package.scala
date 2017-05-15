/*
 * Copyright 2017 Spotify AB.
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

import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode

/**
 * Main package for streaming APIs. Import all.
 *
 * {{{
 * import com.spotify.scio.streaming._
 * }}}
 */
package object streaming {

  /** Alias for WindowingStrategy `AccumulationMode.ACCUMULATING_FIRED_PANES`. */
  val ACCUMULATING_FIRED_PANES = AccumulationMode.ACCUMULATING_FIRED_PANES

  /** Alias for WindowingStrategy `AccumulationMode.DISCARDING_FIRED_PANES`. */
  val DISCARDING_FIRED_PANES = AccumulationMode.DISCARDING_FIRED_PANES

}
