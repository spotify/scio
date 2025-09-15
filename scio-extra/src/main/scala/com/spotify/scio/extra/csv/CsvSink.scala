/*
 * Copyright 2024 Spotify AB.
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

package com.spotify.scio.extra.csv

import kantan.csv.ops.toCsvOutputOps
import kantan.csv.{CsvConfiguration, CsvWriter, HeaderEncoder}
import org.apache.beam.sdk.io.FileIO

import java.io.Writer
import java.nio.channels.{Channels, WritableByteChannel}
import java.nio.charset.StandardCharsets

final private[scio] class CsvSink[T: HeaderEncoder](csvConfig: CsvConfiguration)
    extends FileIO.Sink[T] {
  @transient private var csvWriter: CsvWriter[T] = _
  @transient private var byteChannelWriter: Writer = _

  override def open(channel: WritableByteChannel): Unit = {
    byteChannelWriter = Channels.newWriter(channel, StandardCharsets.UTF_8.name())
    csvWriter = byteChannelWriter.asCsvWriter[T](csvConfig)
  }

  override def write(element: T): Unit = {
    csvWriter.write(element)
    ()
  }

  override def flush(): Unit =
    byteChannelWriter.flush()
}
