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

package com.spotify.scio.coders.instances

import com.spotify.scio.coders.Coder
import scala.reflect.ClassTag
import scala.compiletime._
import scala.deriving._
import scala.quoted._
import org.apache.beam.sdk.coders.AvroCoder
import org.apache.avro.specific.SpecificRecordBase

private object AvroCodersMacros {
  /** Generate a coder which does not serialize the schema and relies exclusively on types. */
  def staticInvokeCoder[T <: SpecificRecordBase](implicit  q: Quotes, t: Type[T]): Expr[Coder[T]] = {
    import quotes.reflect._
    val Some(tag) = Expr.summon[ClassTag[T]]
    val tt = TypeTree.of[T]
    // https://gitter.im/lampepfl/dotty?at=5dbe55a07477946bad1bcbd7
    val const = tt.symbol.primaryConstructor
    val inst = New(tt).select(const).appliedToNone.asExprOf[T]
    '{
      Coder.beam(
        AvroCoder.of[T](
          $tag.runtimeClass.asInstanceOf[Class[T]],
          $inst.getSchema))
    }
  }
}

private[instances] trait AvroCodersMacros {
  // XXX: scala3 - this should not need to be transparent for for some reason
  // it throws the following exception:
  //  [error] java.lang.AssertionError: assertion failed: asTerm called on not-a-Term val <none>
  //  [error] scala.runtime.Scala3RunTime$.assertFailed(Scala3RunTime.scala:8)
  //  [error] dotty.tools.dotc.core.Symbols$Symbol.asTerm(Symbols.scala:156)
  //  [error] dotty.tools.dotc.sbt.ExtractAPICollector.apiDefinition(ExtractAPI.scala:338)
  //  [error] dotty.tools.dotc.sbt.ExtractAPICollector.apiDefinitions$$anonfun$1(ExtractAPI.scala:323)
  //  [error] scala.collection.immutable.List.map(List.scala:250)
  transparent inline implicit def genAvro[T <: SpecificRecordBase]: Any =
    ${ AvroCodersMacros.staticInvokeCoder[T] }
}
