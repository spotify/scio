/*
 * Copyright 2023 Spotify AB.
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

package com.spotify.scio.extra.voyager.syntax

import com.spotify.scio.annotations.experimental
import com.spotify.scio.extra.voyager.{VoyagerReader, VoyagerSideInput, VoyagerUri, VoyagerWriter}
import com.spotify.scio.util.{RemoteFileUtil, ScioUtil}
import com.spotify.scio.values.{SCollection, SideInput}
import com.spotify.voyager.jni.Index.{SpaceType, StorageDataType}
import org.apache.beam.sdk.transforms.View

import java.nio.file.{Files, Paths}
import java.util.UUID

class VoyagerSCollectionOps(@transient private val self: SCollection[VoyagerUri]) extends AnyVal {

  /**
   * Load the Voyager index stored at [[VoyagerUri]] in this
   * [[com.spotify.scio.values.SCollection SCollection]].
   *
   * @return
   *   SideInput[VoyagerReader]
   */
  @experimental
  def asVoyagerSideInput(): SideInput[VoyagerReader] = {
    val view = self.applyInternal(View.asSingleton())
    new VoyagerSideInput(view, RemoteFileUtil.create(self.context.options))
  }

}

class VoyagerPairSCollectionOps(
  @transient private val self: SCollection[(String, Array[Float])]
) extends AnyVal {

  /**
   * Write the key-value pairs of this SCollection as a Voyager index to a specified location using
   * the parameters specified.
   *
   * @param uri
   *   The [[VoyagerUri]].
   * @param spaceType
   *   The measurement for computing distance between entities. One of Euclidean, Cosine or Dot
   *   (inner product).
   * @param storageDataType
   *   The storage data type of the vectors at rest. One of Float8, Float32 or E4M3.
   * @param dim
   *   Number of dimensions in vectors.
   * @param ef
   *   The size of the dynamic list of neighbors used during construction time. This parameter
   *   controls query time/accuracy tradeoff. More information can be found in the hnswlib
   *   documentation https://github.com/nmslib/hnswlib.
   * @param m
   *   The number of outgoing connections in the graph.
   * @return
   *   A [[VoyagerUri]] representing where the index was written to.
   */
  @experimental
  def asVoyager(
    uri: VoyagerUri,
    spaceType: SpaceType,
    storageDataType: StorageDataType,
    dim: Int,
    ef: Long,
    m: Long
  ): SCollection[VoyagerUri] = {
    implicit val remoteFileUtil: RemoteFileUtil = RemoteFileUtil.create(self.context.options)
    require(!uri.exists, s"Voyager URI ${uri.value} already exists")

    self.transform { in =>
      val vectors = in.asIterableSideInput
      self.context
        .parallelize(Seq((): Unit))
        .withSideInputs(vectors)
        .map { case (_, ctx) =>
          val indexUri = uri.value.resolve(VoyagerUri.IndexFile)
          val namesUri = uri.value.resolve(VoyagerUri.NamesFile)
          val isLocal = ScioUtil.isLocalUri(uri.value)

          val (localIndex, localNames) = if (isLocal) {
            (Paths.get(indexUri), Paths.get(namesUri))
          } else {
            val tmpDir = Files.createTempDirectory("voyager-")
            val tmpIndex = tmpDir.resolve(VoyagerUri.IndexFile)
            val tmpNames = tmpDir.resolve(VoyagerUri.NamesFile)
            (tmpIndex, tmpNames)
          }

          val xs = ctx(vectors)
          val writer =
            new VoyagerWriter(localIndex, localNames, spaceType, storageDataType, dim, ef, m)
          writer.write(xs)

          if (!isLocal) {
            remoteFileUtil.upload(localIndex, indexUri)
            remoteFileUtil.upload(localNames, namesUri)
          }

          uri
        }
        .toSCollection
    }
  }

  /**
   * Write the key-value pairs of this SCollection as a Voyager index to a temporary location and
   * building the index using the parameters specified.
   *
   * @param distanceMeasure
   *   The measurement for computing distance between entities. One of Euclidean, Cosine or Dot
   *   (inner product).
   * @param storageDataType
   *   The Storage type of the vectors at rest. One of Float8, Float32 or E4M3.
   * @param dim
   *   Number of dimensions in vectors.
   * @param ef
   *   The size of the dynamic list of neighbors used during construction time. This parameter
   *   controls query time/accuracy tradeoff. More information can be found in the hnswlib
   *   documentation https://github.com/nmslib/hnswlib.
   * @param m
   *   The number of outgoing connections in the graph.
   * @return
   *   A [[VoyagerUri]] representing where the index was written to.
   */
  @experimental
  def asVoyager(
    distanceMeasure: SpaceType,
    storageDataType: StorageDataType,
    dim: Int,
    ef: Long = 200L,
    m: Long = 16L
  ): SCollection[VoyagerUri] = {
    val uuid = UUID.randomUUID()
    val tempLocation: String = self.context.options.getTempLocation
    require(tempLocation != null, s"Voyager writes require --tempLocation to be set.")
    val uri = VoyagerUri(s"${tempLocation.stripSuffix("/")}/voyager-build-$uuid")
    asVoyager(uri, distanceMeasure, storageDataType, dim, ef, m)
  }

  /**
   * Write the key-value pairs of this SCollection as a Voyager index to a temporary location,
   * building the index using the parameters specified and then loading the reader into a side
   * input.
   *
   * @param spaceType
   *   The measurement for computing distance between entities. One of Euclidean, Cosine or Dot
   *   (inner product).
   * @param storageType
   *   The Storage type of the vectors at rest. One of Float8, Float32 or E4M3.
   * @param dim
   *   Number of dimensions in vectors.
   * @param ef
   *   The size of the dynamic list of neighbors used during construction time. This parameter
   *   controls query time/accuracy tradeoff. More information can be found in the hnswlib
   *   documentation https://github.com/nmslib/hnswlib.
   * @param m
   *   The number of outgoing connections in the graph.
   * @return
   *   A SideInput with a [[VoyagerReader]]
   */
  @experimental
  def asVoyagerSideInput(
    spaceType: SpaceType,
    storageType: StorageDataType,
    dim: Int,
    ef: Long = 200L,
    m: Long = 16L
  ): SideInput[VoyagerReader] =
    new VoyagerSCollectionOps(asVoyager(spaceType, storageType, dim, ef, m)).asVoyagerSideInput()
}

trait SCollectionSyntax {
  implicit def voyagerSCollectionOps(coll: SCollection[VoyagerUri]): VoyagerSCollectionOps =
    new VoyagerSCollectionOps(coll)

  implicit def VoyagerPairSCollectionOps(
    coll: SCollection[(String, Array[Float])]
  ): VoyagerPairSCollectionOps =
    new VoyagerPairSCollectionOps(coll)

}
