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

package com.spotify.scio.coders

import com.spotify.scio.coders.instances.kryo.{GenericAvroSerializer, SpecificAvroSerializer}
import com.twitter.chill._
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord

@KryoRegistrar
class AvroKryoRegistrar extends IKryoRegistrar {
  override def apply(k: Kryo): Unit = {
    k.forSubclass[SpecificRecord](new SpecificAvroSerializer)
    k.forSubclass[GenericRecord](new GenericAvroSerializer)
  }

}
