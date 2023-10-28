/*
 * Copyright 2023 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.bigtable

import com.spotify.scio.coders.Coder
import com.spotify.scio.util.ScioUtil
import org.apache.beam.sdk.io.hbase.HBaseCoderProviderRegistrar
import org.apache.beam.sdk.values.TypeDescriptor
import org.apache.hadoop.hbase.client.{Mutation, Result}

import java.util.Collections
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

trait BigtableCoders {

  // FIXME hbase coders are protected. This access is risky
  private val mutationProvider :: _ :: resultProvider :: Nil =
    new HBaseCoderProviderRegistrar().getCoderProviders.asScala.toList

  implicit def mutationCoder[T <: Mutation: ClassTag]: Coder[T] = {
    val td = TypeDescriptor.of(ScioUtil.classOf[T])
    Coder.beam(mutationProvider.coderFor(td, Collections.emptyList()))
  }

  implicit val resultCoder: Coder[Result] =
    Coder.beam(resultProvider.coderFor(TypeDescriptor.of(classOf[Result]), Collections.emptyList()))

}

object BigtableCoders extends BigtableCoders
