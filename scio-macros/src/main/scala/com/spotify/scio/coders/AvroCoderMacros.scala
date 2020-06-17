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

package com.spotify.scio.coders

import org.apache.avro.specific.SpecificRecordBase

import scala.reflect.macros.blackbox

private[coders] object AvroCoderMacros {

  /** Generate a coder which does not serialize the schema and relies exclusively on types. */
  def staticInvokeCoder[T <: SpecificRecordBase: c.WeakTypeTag](c: blackbox.Context): c.Tree = {
    import c.universe._
    val wtt = weakTypeOf[T]
    val companioned = wtt.typeSymbol

    q"""
    _root_.com.spotify.scio.coders.Coder.beam(
      _root_.org.apache.beam.sdk.coders.AvroCoder.of[$companioned](
        classOf[$companioned],
        new $companioned().getSchema
      )
    )
    """
  }
}
