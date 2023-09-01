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
import com.spotify.scio.values.{DistCache, SCollection, SideInput}
import com.spotify.voyager.jni.Index.{SpaceType, StorageDataType}
import com.spotify.voyager.jni.{Index, StringIndex}
import org.apache.beam.sdk.transforms.{DoFn, View}
import org.apache.beam.sdk.values.PCollectionView
import org.slf4j.LoggerFactory

import java.nio.file.{Path, Paths}
import java.util.UUID

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

  class VoyagerReader private[voyager] (
    path: String,
    distanceMeasure: VoyagerDistanceMeasure,
    storageType: VoyagerStorageType,
    dim: Int
  ) {
    require(dim > 0, "Vector dimension should be > 0")

    private val basePath: Path = Paths.get(path)
    private val indexFileName: String = basePath.resolve("index.hnsw").toString
    private val namesFileName: String = basePath.resolve("names.json").toString

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

    def getNearest(v: Array[Float], maxNumResults: Int, ef: Int): Array[VoyagerResult] = {
      val queryResults = index.query(v, maxNumResults, ef)
      queryResults.getNames
        .zip(queryResults.getDistances)
        .map { case (name, distance) =>
          VoyagerResult(name, distance.toFloat)
        }
    }
  }

  /**
   * To be used with with side inputs
   *
   * @param self
   */
  implicit class VoyagerScioContext(private val self: ScioContext) extends AnyVal {
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

    @experimental
    def asVoyager(
      path: String,
      voyagerDistanceMeasure: VoyagerDistanceMeasure,
      voyagerStorageType: VoyagerStorageType,
      dim: Int,
      ef: Long,
      m: Long
    ): SCollection[VoyagerUri] = {
      val uri: VoyagerUri = VoyagerUri(path, self.context.options)
      require(!uri.exists, s"Voyager URI ${uri.path} already exists")
      logger.info(s"Vyager URI :${uri.path}")
      self.transform { in =>
        {
          in.groupBy(_ => ())
            .map { case (_, xs) =>
              val voyagerWriter: VoyagerWriter =
                new VoyagerWriter(voyagerDistanceMeasure, voyagerStorageType, dim, ef, m)

              voyagerWriter.write(xs)
              uri.saveAndClose(voyagerWriter)
              uri
            }
        }
      }
    }

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

  implicit class AnnoySCollection(@transient private val self: SCollection[VoyagerUri])
      extends AnyVal {

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

  private class VoyagerSideInput(
    val view: PCollectionView[VoyagerUri],
    distanceMeasure: VoyagerDistanceMeasure,
    storageType: VoyagerStorageType,
    dim: Int
  ) extends SideInput[VoyagerReader] {
    override def get[I, O](context: DoFn[I, O]#ProcessContext): VoyagerReader =
      context.sideInput(view).getReader(distanceMeasure, storageType, dim)
  }

}
