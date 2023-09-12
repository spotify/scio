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
package com.spotify.scio.extra

import com.spotify.scio.ScioContext
import com.spotify.scio.annotations.experimental
import com.spotify.scio.values.{SCollection, SideInput}
import com.spotify.voyager.jni.Index.{SpaceType, StorageDataType}
import com.spotify.voyager.jni.StringIndex
import org.apache.beam.sdk.transforms.{DoFn, View}
import org.apache.beam.sdk.values.PCollectionView
import org.slf4j.LoggerFactory

import java.util.UUID
import scala.collection.mutable

/** Main package for Voyager side input APIs. Import all. */
package object voyager {
  sealed abstract class VoyagerDistanceMeasure
  case object Euclidean extends VoyagerDistanceMeasure
  case object Cosine extends VoyagerDistanceMeasure
  case object Dot extends VoyagerDistanceMeasure

  sealed abstract class VoyagerStorageType
  case object Float8 extends VoyagerStorageType
  case object Float32 extends VoyagerStorageType
  case object E4M3 extends VoyagerStorageType
  private val logger = LoggerFactory.getLogger(this.getClass)

  case class VoyagerResult(value: String, distance: Float)

  val VOYAGER_URI_MAP: mutable.Map[VoyagerUri, VoyagerReader] = mutable.HashMap.empty

  /**
   * Voyager reader class for nearest neighbor lookups. Supports looking up neighbors for a vector
   * and returning the string labels and distances associated.
   *
   * @param indexFileName
   *   The path to the `index.hnsw` local or remote file.
   * @param namesFileName
   *   The path to the `names.json` local or remote file.
   * @param distanceMeasure
   *   The measurement for computing distance between entities. One of Euclidean, Cosine or Dot
   *   (inner product).
   * @param storageType
   *   The Storage type of the vectors at rest. One of Float8, Float32 or E4M3.
   * @param dim
   *   Number of dimensions in vectors.
   */
  class VoyagerReader private[voyager] (
    indexFileName: String,
    namesFileName: String,
    distanceMeasure: VoyagerDistanceMeasure,
    storageType: VoyagerStorageType,
    dim: Int
  ) {
    require(dim > 0, "Vector dimension should be > 0")

    private val index: StringIndex = {
      val spaceType = distanceMeasure match {
        case Euclidean => SpaceType.Euclidean
        case Cosine    => SpaceType.Cosine
        case Dot       => SpaceType.InnerProduct
      }

      val storageDataType = storageType match {
        case Float8  => StorageDataType.Float8
        case Float32 => StorageDataType.Float32
        case E4M3    => StorageDataType.E4M3
      }
      StringIndex.load(indexFileName, namesFileName, spaceType, dim, storageDataType)
    }

    /**
     * Gets maxNumResults nearest neighbors for vector v using ef (where ef is the size of the
     * dynamic list for the nearest neighbors during search).
     */
    def getNearest(v: Array[Float], maxNumResults: Int, ef: Int): Array[VoyagerResult] = {
      val queryResults = index.query(v, maxNumResults, ef)
      queryResults.getNames
        .zip(queryResults.getDistances)
        .map { case (name, distance) =>
          VoyagerResult(name, distance)
        }
    }
  }

  /** Enhanced version of [[ScioContext]] with Voyager methods */
  implicit class VoyagerScioContext(private val self: ScioContext) extends AnyVal {

    /**
     * Creates a SideInput of [[VoyagerReader]] from an [[VoyagerUri]] base path. To be used with
     * [[com.spotify.scio.values.SCollection.withSideInputs SCollection.withSideInputs]]
     *
     * @param path
     *   The directory path to be used for the [[VoyagerUri]].
     * @param distanceMeasure
     *   The measurement for computing distance between entities. One of Euclidean, Cosine or Dot
     *   (inner product).
     * @param storageType
     *   The Storage type of the vectors at rest. One of Float8, Float32 or E4M3.
     * @param dim
     *   Number of dimensions in vectors.
     * @return
     *   A [[SideInput]] of the [[VoyagerReader]] to be used for querying.
     */
    @experimental
    def voyagerSideInput(
      path: String,
      distanceMeasure: VoyagerDistanceMeasure,
      storageType: VoyagerStorageType,
      dim: Int
    ): SideInput[VoyagerReader] = {
      val uri = VoyagerUri(path, self.options)
      val view = self.parallelize(Seq(uri)).applyInternal(View.asSingleton())
      new VoyagerSideInput(view, distanceMeasure, storageType, dim)
    }
  }

  implicit class VoyagerPairSCollection(
    @transient private val self: SCollection[(String, Array[Float])]
  ) extends AnyVal {

    /**
     * Write the key-value pairs of this SCollection as a Voyager index to a specified location
     * using the parameters specified.
     *
     * @param path
     *   The directory path to be used for the [[VoyagerUri]].
     * @param distanceMeasure
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
     *   A [[VoyagerUri]] representing where the index was written to.
     */
    @experimental
    def asVoyager(
      path: String,
      distanceMeasure: VoyagerDistanceMeasure,
      storageType: VoyagerStorageType,
      dim: Int,
      ef: Long,
      m: Long
    ): SCollection[VoyagerUri] = {
      val uri: VoyagerUri = VoyagerUri(path, self.context.options)
      require(!uri.exists, s"Voyager URI ${uri.path} already exists")
      logger.info(s"Voyager URI :${uri.path}")
      self.transform { in =>
        {
          in.groupBy(_ => ())
            .map { case (_, xs) =>
              val voyagerWriter: VoyagerWriter =
                new VoyagerWriter(distanceMeasure, storageType, dim, ef, m)

              voyagerWriter.write(xs)
              uri.saveAndClose(voyagerWriter)
              uri
            }
        }
      }
    }

    /**
     * Write the key-value pairs of this SCollection as a Voyager index to a temporary location and
     * building the index using the parameters specified.
     *
     * @param distanceMeasure
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
     *   A [[VoyagerUri]] representing where the index was written to.
     */
    @experimental
    def asVoyager(
      distanceMeasure: VoyagerDistanceMeasure,
      storageType: VoyagerStorageType,
      dim: Int,
      ef: Long = 200L,
      m: Long = 16L
    ): SCollection[VoyagerUri] = {
      val uuid: UUID = UUID.randomUUID()
      val tempLocation: String = self.context.options.getTempLocation
      require(tempLocation != null, s"--tempLocation arg is required")
      val path = s"$tempLocation/voyager-build-$uuid"
      this.asVoyager(path, distanceMeasure, storageType, dim, ef, m)
    }

    /**
     * Write the key-value pairs of this SCollection as a Voyager index to a temporary location,
     * building the index using the parameters specified and then loading the reader into a side
     * input.
     *
     * @param distanceMeasure
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
      distanceMeasure: VoyagerDistanceMeasure,
      storageType: VoyagerStorageType,
      dim: Int,
      ef: Long = 200L,
      m: Long = 16L
    ): SideInput[VoyagerReader] =
      self
        .asVoyager(distanceMeasure, storageType, dim, ef, m)
        .asVoyagerSideInput(distanceMeasure, storageType, dim)
  }

  /**
   * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with Voyager methods.
   */
  implicit class AnnoySCollection(@transient private val self: SCollection[VoyagerUri])
      extends AnyVal {

    /**
     * Load the Voyager index stored at [[VoyagerUri]] in this
     * [[com.spotify.scio.values.SCollection SCollection]].
     *
     * @param distanceMeasure
     *   The measurement for computing distance between entities. One of Euclidean, Cosine or Dot
     *   (inner product).
     * @param storageType
     *   The Storage type of the vectors at rest. One of Float8, Float32 or E4M3.
     * @param dim
     *   Number of dimensions in vectors.
     * @return
     *   SideInput[VoyagerReader]
     */
    @experimental
    def asVoyagerSideInput(
      distanceMeasure: VoyagerDistanceMeasure,
      storageType: VoyagerStorageType,
      dim: Int
    ): SideInput[VoyagerReader] = {
      val view = self.applyInternal(View.asSingleton())
      new VoyagerSideInput(view, distanceMeasure, storageType, dim)
    }
  }

  /**
   * Construction for a VoyagerSide input that leverages a synchronized map to ensure that the
   * reader is only loaded once per [[VoyagerUri]].
   */
  private class VoyagerSideInput(
    val view: PCollectionView[VoyagerUri],
    distanceMeasure: VoyagerDistanceMeasure,
    storageType: VoyagerStorageType,
    dim: Int
  ) extends SideInput[VoyagerReader] {
    override def get[I, O](context: DoFn[I, O]#ProcessContext): VoyagerReader = {
      val uri = context.sideInput(view)
      VOYAGER_URI_MAP.synchronized {
        if (!VOYAGER_URI_MAP.contains(uri)) {
          VOYAGER_URI_MAP.put(uri, uri.getReader(distanceMeasure, storageType, dim))
        }
        VOYAGER_URI_MAP(uri)
      }

    }
  }

}
