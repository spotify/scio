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

package com.spotify.scio.jmh

import com.spotify.scio.{ScioContext, ScioResult}
import com.spotify.scio.avro._
import com.spotify.scio.coders._
import org.apache.beam.sdk.coders.{KvCoder, Coder => BCoder}
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.transforms.GroupByKey
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import java.util.concurrent.TimeUnit
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.openjdk.jmh.annotations._
import scala.collection.JavaConverters._

// scalastyle:off number.of.methods
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
class GroupByBenchmark {

  val schema =
    """
      {
        "type": "record",
        "name": "Event",
        "namespace": "smbjoin",
        "fields": [
          {
            "name": "id",
            "type": "string"
          },
          {
            "name": "value",
            "type": "double"
          }
        ]
      }
    """

  val avroSchema =
    new Schema.Parser().parse(schema)

  private def runWithContext[T](fn: ScioContext => T): ScioResult = {
    val opts = PipelineOptionsFactory.as(classOf[PipelineOptions])
    val sc = ScioContext(opts)
    fn(sc)
    sc.close()
  }

  val source = "src/test/resources/events-10000-0.avro"
  implicit val coderGenericRecord: Coder[GenericRecord] =
    Coder.avroGenericRecordCoder(avroSchema)

  val charCoder = CoderMaterializer.beamWithDefault(Coder[Char])
  val doubleCoder = CoderMaterializer.beamWithDefault(Coder[Double])
  val kvCoder: BCoder[KV[Char, Double]] = KvCoder.of(charCoder, doubleCoder)

  @Benchmark
  def testScioGroupByKey =
    runWithContext { sc =>
      sc.avroFile[GenericRecord](source, schema = avroSchema)
        .map { rec =>
          (rec.get("id").toString.head, rec.get("value").asInstanceOf[Double])
        }
        .groupByKey
    }

  @Benchmark
  def testBeamGroupByKey =
    runWithContext { sc =>
      sc.wrap {
          sc.avroFile[GenericRecord](source, schema = avroSchema)
            .map { rec =>
              KV.of(rec.get("id").toString.head, rec.get("value").asInstanceOf[Double])
            }
            .internal
            .setCoder(kvCoder)
            .apply(GroupByKey.create[Char, Double])
        }
        .map { kv =>
          (kv.getKey, kv.getValue.asScala)
        }
    }
}
