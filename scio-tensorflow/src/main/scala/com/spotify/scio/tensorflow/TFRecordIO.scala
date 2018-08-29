/*
 * Copyright 2018 Spotify AB.
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

package com.spotify.scio.tensorflow

import com.spotify.scio.ScioContext
import com.spotify.scio.io.Tap
import com.spotify.scio.nio.ScioIO
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.Compression
import org.apache.beam.sdk.{io => bio}
import org.tensorflow.example.Example

import scala.concurrent.Future

final case class TFRecordIO(path: String) extends ScioIO[Array[Byte]] {
  override type ReadP = TFRecordIO.ReadParam
  override type WriteP = TFRecordIO.WriteParam

  override def read(sc: ScioContext, params: ReadP): SCollection[Array[Byte]] =
    TFRecordMethods.read(sc, path, params)

  override def write(data: SCollection[Array[Byte]], params: WriteP): Future[Tap[Array[Byte]]] = {
    TFRecordMethods.write(data, path, params)
    data.context.makeFuture(tap(TFRecordIO.ReadParam(params.compression)))
  }

  override def tap(params: ReadP): Tap[Array[Byte]] = TFRecordMethods.tap(params, path)
}

object TFRecordIO {
  final case class ReadParam(compression: Compression = Compression.AUTO)
  final case class WriteParam(suffix: String = ".tfrecords",
                              compression: Compression = Compression.UNCOMPRESSED,
                              numShards: Int = 0)
}

final case class TFExampleIO(path: String) extends ScioIO[Example] {
  override type ReadP = TFExampleIO.ReadParam
  override type WriteP = TFExampleIO.WriteParam

  override def read(sc: ScioContext, params: ReadP): SCollection[Example] =
    TFRecordMethods.read(sc, path, params).map(Example.parseFrom)

  override def write(data: SCollection[Example], params: WriteP): Future[Tap[Example]] = {
    TFRecordMethods.write(data.map(_.toByteArray), path, params)
    data.context.makeFuture(tap(TFExampleIO.ReadParam(params.compression)))
  }

  override def tap(params: ReadP): Tap[Example] =
    TFRecordMethods.tap(params, path).map(Example.parseFrom)
}

object TFExampleIO {
  type ReadParam = TFRecordIO.ReadParam
  type WriteParam = TFRecordIO.WriteParam
  val ReadParam = TFRecordIO.ReadParam
  val WriteParam = TFRecordIO.WriteParam
}

private object TFRecordMethods {

  def read(sc: ScioContext,
           path: String,
           params: TFRecordIO.ReadParam): SCollection[Array[Byte]] =
    sc.wrap(sc.applyInternal(bio.TFRecordIO
      .read()
      .from(path)
      .withCompression(params.compression)))

  def write(data: SCollection[Array[Byte]],
            path: String,
            params: TFRecordIO.WriteParam): Unit =
    data.applyInternal(bio.TFRecordIO
      .write()
      .to(data.pathWithShards(path))
      .withSuffix(params.suffix)
      .withCompression(params.compression)
      .withNumShards(params.numShards))

  def tap(read: TFRecordIO.ReadParam, path: String): Tap[Array[Byte]] =
    TFRecordFileTap(ScioUtil.addPartSuffix(path))
}
