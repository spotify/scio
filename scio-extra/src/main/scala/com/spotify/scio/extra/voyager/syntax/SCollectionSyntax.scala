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
   * @param space
   *   The measurement for computing distance between entities. One of Euclidean, Cosine or Dot
   *   (inner product).
   * @param numDimensions
   *   Number of dimensions in vectors.
   * @param storageDataType
   *   The Storage type of the vectors at rest. One of Float8, Float32 or E4M3.
   * @return
   *   SideInput[VoyagerReader]
   */
  @experimental
  def asVoyagerSideInput(
    space: SpaceType,
    numDimensions: Int,
    storageDataType: StorageDataType
  ): SideInput[VoyagerReader] = {
    val settings = VoyagerReader.ProvidedSettings(space, numDimensions, storageDataType)
    val remoteFileUtil = RemoteFileUtil.create(self.context.options)
    val view = self.applyInternal(View.asSingleton())
    new VoyagerSideInput(settings, remoteFileUtil, view)
  }

  /**
   * Load the Voyager v2 index stored at [[VoyagerUri]] in this
   * [[com.spotify.scio.values.SCollection SCollection]].
   *
   * @return
   *   SideInput[VoyagerReader]
   */
  @experimental
  def asVoyagerSideInput(): SideInput[VoyagerReader] = {
    val remoteFileUtil = RemoteFileUtil.create(self.context.options)
    val view = self.applyInternal(View.asSingleton())
    new VoyagerSideInput(VoyagerReader.MetadataSettings, remoteFileUtil, view)
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
   * @param space
   *   The space type to use when storing and comparing vectors.
   * @param numDimensions
   *   The number of dimensions per vector.
   * @param indexM
   *   Controls the degree of interconnectedness between vectors.
   * @param efConstruction
   *   Controls index quality, affecting the speed of addItem calls. Does not affect memory usage or
   *   size of the index.
   * @param randomSeed
   *   A random seed to use when initializing the index's internal data structures.
   * @param storageDataType
   *   The datatype to use under-the-hood when storing vectors.
   * @return
   *   A [[VoyagerUri]] representing where the index was written to.
   */
  @experimental
  def asVoyager(
    uri: VoyagerUri,
    space: SpaceType,
    numDimensions: Int,
    indexM: Long = VoyagerWriter.DefaultIndexM,
    efConstruction: Long = VoyagerWriter.DefaultEfConstruction,
    randomSeed: Long = VoyagerWriter.DefaultRandomSeed,
    storageDataType: StorageDataType = VoyagerWriter.DefaultStorageDataType
  ): SCollection[VoyagerUri] = {
    implicit val remoteFileUtil: RemoteFileUtil = RemoteFileUtil.create(self.context.options)
    require(!uri.exists, s"Voyager URI ${uri.value} already exists")

    self.transform { in =>
      val count = in.count.asSingletonSideInput
      in
        .groupBy(_ => ()) // asIterableSideInput fails for large indexes
        .withSideInputs(count)
        .map { case ((_, xs), ctx) =>
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

          val maxElements = ctx(count)
          val settings = VoyagerWriter.IndexSettings(
            space,
            numDimensions,
            indexM,
            efConstruction,
            randomSeed,
            maxElements,
            storageDataType
          )
          val writer = new VoyagerWriter(localIndex, localNames, settings)
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
   */
  private[voyager] def asVoyager(
    space: SpaceType,
    numDimensions: Int,
    indexM: Long,
    efConstruction: Long,
    randomSeed: Long,
    storageDataType: StorageDataType
  ): SCollection[VoyagerUri] = {
    val uuid = UUID.randomUUID()
    val tempLocation: String = self.context.options.getTempLocation
    require(tempLocation != null, s"Voyager writes require --tempLocation to be set.")
    val uri = VoyagerUri(s"${tempLocation.stripSuffix("/")}/voyager-build-$uuid")
    asVoyager(
      uri,
      space,
      numDimensions,
      indexM,
      efConstruction,
      randomSeed,
      storageDataType
    )
  }

  /**
   * Write the key-value pairs of this SCollection as a Voyager index to a temporary location,
   * building the index using the parameters specified and then loading the reader into a side
   * input.
   *
   * @param space
   *   The space type to use when storing and comparing vectors.
   * @param numDimensions
   *   The number of dimensions per vector.
   * @param indexM
   *   Controls the degree of interconnectedness between vectors.
   * @param efConstruction
   *   Controls index quality, affecting the speed of addItem calls. Does not affect memory usage or
   *   size of the index.
   * @param randomSeed
   *   A random seed to use when initializing the index's internal data structures.
   * @param storageDataType
   *   The datatype to use under-the-hood when storing vectors.
   * @return
   *   A SideInput with a [[VoyagerReader]]
   */
  @experimental
  def asVoyagerSideInput(
    space: SpaceType,
    numDimensions: Int,
    indexM: Long = VoyagerWriter.DefaultIndexM,
    efConstruction: Long = VoyagerWriter.DefaultEfConstruction,
    randomSeed: Long = VoyagerWriter.DefaultRandomSeed,
    storageDataType: StorageDataType = VoyagerWriter.DefaultStorageDataType
  ): SideInput[VoyagerReader] = {
    val voyagerUri = asVoyager(
      space,
      numDimensions,
      indexM,
      efConstruction,
      randomSeed,
      storageDataType
    )
    new VoyagerSCollectionOps(voyagerUri).asVoyagerSideInput()
  }
}

trait SCollectionSyntax {
  implicit def voyagerSCollectionOps(coll: SCollection[VoyagerUri]): VoyagerSCollectionOps =
    new VoyagerSCollectionOps(coll)

  implicit def VoyagerPairSCollectionOps(
    coll: SCollection[(String, Array[Float])]
  ): VoyagerPairSCollectionOps =
    new VoyagerPairSCollectionOps(coll)

}
