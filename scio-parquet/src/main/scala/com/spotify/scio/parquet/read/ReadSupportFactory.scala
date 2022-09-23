/*
 * Copyright 2022 Spotify AB
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

package com.spotify.scio.parquet.read

import magnolify.parquet.ParquetType
import me.lyh.parquet.tensorflow.ExampleReadSupport
import org.apache.parquet.avro.AvroReadSupport
import org.apache.parquet.hadoop.api.ReadSupport
import org.tensorflow.proto.example.Example

sealed trait ReadSupportFactory[T] extends Serializable {
  def readSupport: ReadSupport[T]
}

object ReadSupportFactory {
  def typed[T](implicit pt: ParquetType[T]): ReadSupportFactory[T] = new ReadSupportFactory[T] {
    def readSupport: ReadSupport[T] = pt.readSupport
  }

  def avro[T]: ReadSupportFactory[T] = new ReadSupportFactory[T] {
    def readSupport: ReadSupport[T] = new AvroReadSupport
  }

  def example: ReadSupportFactory[Example] = new ReadSupportFactory[Example] {
    def readSupport: ReadSupport[Example] = new ExampleReadSupport()
  }
}
