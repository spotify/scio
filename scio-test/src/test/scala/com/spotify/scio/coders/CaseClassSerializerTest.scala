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

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ByteBufferInput, FastOutput}
import com.spotify.scio.testing.PipelineSpec

import scala.reflect.ClassTag

class CaseClassSerializerTest extends PipelineSpec {

  val kryo = () => new KryoAtomicCoder[Any](KryoOptions()).kryo().kryo

  private def ccs[T:ClassTag](kryo: Kryo) = {
    new CaseClassSerializer[T](kryo)
  }

  "KryoAtomicCoder" should "support Scala collections" in {
    val instnace = kryo()
    instnace.addDefaultSerializer(classOf[Sample], new CaseClassSerializer[Sample](instnace))
    instnace.newInstance(classOf[Sample]) should not be null
    val output  = new FastOutput(0, 1024)
    instnace.writeObject(output, Sample(1, "2"))
    val input = instnace.readObject(new ByteBufferInput(output.getBuffer), classOf[Sample])
    input should be equals Sample(1, "2")
  }

  case class Sample(one: Int, two: String)

}
