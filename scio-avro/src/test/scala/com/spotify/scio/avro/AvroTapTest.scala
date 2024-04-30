/*
 * Copyright 2024 Spotify AB
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

package com.spotify.scio.avro

import com.spotify.scio.ScioContext
import com.spotify.scio.avro.AvroUtils._
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.TapSpec
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import com.spotify.scio.options.ScioOptions
import com.spotify.scio.proto.SimpleV2.{SimplePB => SimplePBV2}
import com.spotify.scio.proto.SimpleV3.{SimplePB => SimplePBV3}

import java.nio.ByteBuffer

class AvroTapTest extends TapSpec {
  val schema: Schema = AvroUtils.schema
  implicit def coder: Coder[GenericRecord] = avroGenericRecordCoder(schema)

  "AvroTap" should "support saveAsAvroFile with SpecificRecord" in withTempDir { dir =>
    val t = runWithFileFuture {
      _.parallelize(Seq(1, 2, 3))
        .map(newSpecificRecord)
        .saveAsAvroFile(dir.getAbsolutePath)
    }
    verifyTap(t, Set(1, 2, 3).map(newSpecificRecord))
  }

  it should "support saveAsAvroFile with GenericRecord" in withTempDir { dir =>
    val t = runWithFileFuture {
      _.parallelize(Seq(1, 2, 3))
        .map(newGenericRecord)
        .saveAsAvroFile(dir.getAbsolutePath, schema = schema)
    }
    verifyTap(t, Set(1, 2, 3).map(newGenericRecord))
  }

  it should "support saveAsAvroFile with reflect record" in withTempDir { dir =>
    import com.spotify.scio.coders.AvroBytesUtil
    implicit val coder = avroGenericRecordCoder(AvroBytesUtil.schema)

    val tap = runWithFileFuture {
      _.parallelize(Seq("a", "b", "c"))
        .map[GenericRecord] { s =>
          new GenericRecordBuilder(AvroBytesUtil.schema)
            .set("bytes", ByteBuffer.wrap(s.getBytes))
            .build()
        }
        .saveAsAvroFile(dir.getAbsolutePath, schema = AvroBytesUtil.schema)
    }

    val result = tap
      .map { gr =>
        val bb = gr.get("bytes").asInstanceOf[ByteBuffer]
        new String(bb.array(), bb.position(), bb.limit())
      }

    verifyTap(result, Set("a", "b", "c"))
  }

  it should "support saveAsProtobuf proto version 2" in withTempDir { dir =>
    val data = Seq(("a", 1L), ("b", 2L), ("c", 3L))
    // use java protos otherwise we would have to pull in pb-scala
    def mkProto(t: (String, Long)): SimplePBV2 =
      SimplePBV2
        .newBuilder()
        .setPlays(t._2)
        .setTrackId(t._1)
        .build()
    val t = runWithFileFuture {
      _.parallelize(data)
        .map(mkProto)
        .saveAsProtobufFile(dir.getAbsolutePath)
    }
    val expected = data.map(mkProto).toSet
    verifyTap(t, expected)
  }

  // use java protos otherwise we would have to pull in pb-scala
  private def mkProto3(t: (String, Long)): SimplePBV3 =
    SimplePBV3
      .newBuilder()
      .setPlays(t._2)
      .setTrackId(t._1)
      .build()

  it should "support saveAsProtobuf proto version 3" in withTempDir { dir =>
    val data = Seq(("a", 1L), ("b", 2L), ("c", 3L))
    val t = runWithFileFuture {
      _.parallelize(data)
        .map(mkProto3)
        .saveAsProtobufFile(dir.getAbsolutePath)
    }
    val expected = data.map(mkProto3).toSet
    verifyTap(t, expected)
  }

  it should "support saveAsProtobuf write with nullableCoders" in withTempDir { dir =>
    val data = Seq(("a", 1L), ("b", 2L), ("c", 3L))
    val actual = data.map(mkProto3)
    val t = runWithFileFuture { sc =>
      sc.optionsAs[ScioOptions].setNullableCoders(true)
      sc.parallelize(actual)
        .saveAsProtobufFile(dir.getAbsolutePath)
    }
    val expected = actual.toSet
    verifyTap(t, expected)

    val sc = ScioContext()
    sc.protobufFile[SimplePBV3](
      path = dir.getAbsolutePath,
      suffix = ".protobuf.avro"
    ) should containInAnyOrder(expected)
    sc.run()
  }

  it should "support saveAsProtobuf read with nullableCoders" in withTempDir { dir =>
    val data = Seq(("a", 1L), ("b", 2L), ("c", 3L))
    val actual = data.map(mkProto3)
    val t = runWithFileFuture {
      _.parallelize(actual)
        .saveAsProtobufFile(dir.getAbsolutePath)
    }
    val expected = actual.toSet
    verifyTap(t, expected)

    val sc = ScioContext()
    sc.optionsAs[ScioOptions].setNullableCoders(true)
    sc.protobufFile[SimplePBV3](
      path = dir.getAbsolutePath,
      suffix = ".protobuf.avro"
    ) should containInAnyOrder(expected)
    sc.run()
  }
}
