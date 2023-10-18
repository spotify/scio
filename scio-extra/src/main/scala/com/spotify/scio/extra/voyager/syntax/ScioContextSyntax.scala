/*
 * Copyright 2023 Spotify AB.
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

package com.spotify.scio.extra.voyager.syntax

import com.spotify.scio.ScioContext
import com.spotify.scio.annotations.experimental
import com.spotify.scio.extra.voyager.{VoyagerReader, VoyagerSideInput, VoyagerUri}
import com.spotify.scio.util.RemoteFileUtil
import com.spotify.scio.values.SideInput
import org.apache.beam.sdk.transforms.View

/** Enhanced version of [[ScioContext]] with Voyager methods */
class VoyagerScioContextOps(private val self: ScioContext) extends AnyVal {

  /**
   * Creates a SideInput of [[VoyagerReader]] from an [[VoyagerUri]] base path. To be used with
   * [[com.spotify.scio.values.SCollection.withSideInputs SCollection.withSideInputs]]
   *
   * @param uri
   *   The [[VoyagerUri]] base path.
   * @return
   *   A [[SideInput]] of the [[VoyagerReader]] to be used for querying.
   */
  @experimental
  def voyagerSideInput(
    uri: VoyagerUri
  ): SideInput[VoyagerReader] = {
    val view = self.parallelize(Seq(uri)).applyInternal(View.asSingleton())
    new VoyagerSideInput(view, RemoteFileUtil.create(self.options))
  }
}

trait ScioContextSyntax {
  implicit def voyagerScioContextOps(sc: ScioContext): VoyagerScioContextOps =
    new VoyagerScioContextOps(sc)
}
