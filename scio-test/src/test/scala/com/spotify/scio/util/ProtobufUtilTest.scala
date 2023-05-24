/*
 * Copyright 2020 Spotify AB.
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

package com.spotify.scio.util

import java.io.File
import java.nio.channels.Channels
import java.nio.file.Files

import com.spotify.scio.ScioContext
import com.spotify.scio.avro._
import com.spotify.scio.coders.Coder
import com.spotify.scio.proto.Track.TrackPB
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.beam.sdk.io.{FileSystems, LocalResources}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class ProtobufUtilTest extends AnyFlatSpec with Matchers {

  "ProtobufUtil" should "convert Message -> GenericRecords that can be written and read" in {
    val sc = ScioContext()
    val dir = Files.createTempDirectory("protobuf-util-")
    val (path1, path2) = (new File(s"$dir/1"), new File(s"$dir/2"))
    path1.deleteOnExit()
    path2.deleteOnExit()
    dir.toFile.deleteOnExit()

    implicit val grCoder: Coder[GenericRecord] = ProtobufUtil.AvroMessageCoder

    val messages = sc
      .parallelize(1 to 10)
      .map(i => TrackPB.newBuilder().setTrackId(i.toString).build())

    messages
      .map(ProtobufUtil.toAvro[TrackPB])
      .saveAsAvroFile(
        path1.getPath,
        suffix = ".protobuf",
        metadata = ProtobufUtil.schemaMetadataOf[TrackPB],
        schema = ProtobufUtil.AvroMessageSchema,
        numShards = 1
      )

    val protoWriteTap = messages.saveAsProtobufFile(path2.getPath, numShards = 1)

    val result = sc.run().waitUntilDone()

    val (tapFromAvroWrite, tapFromProtoWrite) = (
      ObjectFileTap[TrackPB](path1.getPath, ObjectFileIO.ReadParam(".protobuf")),
      protoWriteTap.get(result)
    )

    tapFromAvroWrite.value.toList should contain theSameElementsAs tapFromProtoWrite.value.toList
    getMetadata(path1) should contain theSameElementsAs getMetadata(path2)
  }

  private def getMetadata(dir: File): Map[String, AnyRef] = {
    val files = dir.listFiles()
    if (files.length != 1) {
      fail(s"Directory $dir should contain 1 Avro file. Instead, found ${files.toList}")
    }

    val dfs = new DataFileStream[GenericRecord](
      Channels.newInputStream(FileSystems.open(LocalResources.fromFile(files(0), false))),
      new GenericDatumReader[GenericRecord]
    )

    dfs.getMetaKeys.asScala.map(k => (k, dfs.getMetaString(k))).toMap
  }
}
