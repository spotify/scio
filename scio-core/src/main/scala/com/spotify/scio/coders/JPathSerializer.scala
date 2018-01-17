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

package com.spotify.scio.coders

import com.twitter.chill.KSerializer
import java.nio.file.{Path, Paths}

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}

private class JPathSerializer extends KSerializer[Path] {
  override def read(kryo: Kryo, input: Input, tpe: Class[Path]): Path =
    Paths.get(input.readString())

  override def write(kryo: Kryo, output: Output, path: Path): Unit =
    output.writeString(path.toString)
}
