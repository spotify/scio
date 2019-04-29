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
package com.spotify.scio.schemas.instances

import com.spotify.scio.schemas.{RawRecord, Schema}
import org.apache.avro.specific.SpecificRecord
import org.apache.beam.sdk.schemas.utils.AvroUtils
import org.apache.beam.sdk.schemas.{AvroRecordSchema, Schema => BSchema}
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.values.{Row, TypeDescriptor}

import scala.collection.JavaConverters._
import scala.reflect.{classTag, ClassTag}

trait AvroInstances {

  implicit def avroSchema[T <: SpecificRecord: ClassTag]: Schema[T] = {
    // TODO: broken because of a bug upstream https://issues.apache.org/jira/browse/BEAM-6742
    // RawRecord[T](new AvroRecordSchema())
    import org.apache.avro.reflect.ReflectData
    val rc = classTag[T].runtimeClass.asInstanceOf[Class[T]]
    val provider = new AvroRecordSchema()
    val td = TypeDescriptor.of(rc)
    val schema = provider.schemaFor(td)
    val avroSchema =
      new AvroInstances.SerializableSchema(ReflectData.get().getSchema(td.getRawType))

    def fromRow = provider.fromRowFunction(td)

    val toRow: SerializableFunction[T, Row] =
      new SerializableFunction[T, Row] {
        def apply(t: T): Row =
          AvroInstances.specificRecordtoRow(schema, avroSchema, t)
      }
    RawRecord[T](schema, fromRow, toRow)
  }

}

object AvroInstances {
  private class SerializableSchema(@transient private val schema: org.apache.avro.Schema)
      extends Serializable {
    private[this] val stringSchema = schema.toString
    def get: org.apache.avro.Schema = new org.apache.avro.Schema.Parser().parse(stringSchema)
  }

  // Workaround BEAM-6742
  private def specificRecordtoRow[T <: SpecificRecord](
    schema: BSchema,
    avroSchema: SerializableSchema,
    t: T
  ): Row = {
    val row = Row.withSchema(schema)
    schema.getFields.asScala.zip(avroSchema.get.getFields.asScala).zipWithIndex.foreach {
      case ((f, a), i) =>
        val value = t.get(i)
        val v = AvroUtils.convertAvroFieldStrict(value, a.schema, f.getType)
        row.addValue(v)
    }
    row.build()
  }
}
