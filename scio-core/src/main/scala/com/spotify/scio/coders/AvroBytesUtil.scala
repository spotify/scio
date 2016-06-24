/*
 * Copyright 2016 Spotify AB.
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

import java.nio.ByteBuffer

import com.google.cloud.dataflow.sdk.coders.Coder
import com.google.cloud.dataflow.sdk.util.CoderUtils
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}

import scala.collection.JavaConverters._

private[scio] object AvroBytesUtil {

  val schema: Schema = {
    val s = Schema.createRecord("AvroBytesRecord", null, null, false)
    s.setFields(List(
      new Schema.Field("bytes", Schema.create(Schema.Type.BYTES), null, null)
    ).asJava)
    s
  }

  def encode[T](coder: Coder[T], obj: T): GenericRecord = {
    val bytes = CoderUtils.encodeToByteArray(coder, obj)
    val record = new GenericData.Record(schema)
    record.put("bytes", ByteBuffer.wrap(bytes))
    record
  }

  def decode[T](coder: Coder[T], record: GenericRecord): T = {
    val bytes = record.get("bytes").asInstanceOf[ByteBuffer].array()
    CoderUtils.decodeFromByteArray(coder, bytes)
  }

}
