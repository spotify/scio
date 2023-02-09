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

import com.spotify.scio.smb.syntax.AllSyntax
import org.apache.beam.sdk.values.TupleTag

package object smb extends AllSyntax {
  private[smb] def validateTupleTags(tupleTags: TupleTag[_]*): Unit = {
    if (tupleTags.distinctBy(_.getId).length != tupleTags.length) {
      throw new IllegalArgumentException(
        "Tuple tag name for each smb input source needs to be unique"
      )
    }
  }
}

