package com.spotify.scio.extra

import com.spotify.scio.annotations.experimental
import com.spotify.scio.values.DistCache
import com.spotify.voyager.jni.Index.{SpaceType, StorageDataType}
import com.spotify.voyager.jni.Index
import org.apache.beam.sdk.values.PCollectionView

package object voyager {
  sealed abstract class VoyagerDistanceMeasure
  case object Euclidean extends VoyagerDistanceMeasure
  case object Cosine extends VoyagerDistanceMeasure
  case object Dot extends VoyagerDistanceMeasure

  sealed abstract class VoyagerStorageType
  case object Float8 extends VoyagerStorageType
  case object Float32 extends VoyagerStorageType
  case object E4M3 extends VoyagerStorageType

  class VoyagerReader private[voyager] (
    path: String,
    distanceMeasure: VoyagerDistanceMeasure,
    storageType: VoyagerStorageType,
    dim: Int
  ) {
    require(dim > 0, "Vector dimension should be > 0")
    private val index: Index = {
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
      Index.load(path, spaceType, dim, storageDataType)
    }

    def getNearest(v: Array[Float], maxNumResults: Int) = index.query(v, maxNumResults)

    def getItemVector(i: Int) = index.getVector(i)
  }

  @experimental
  def asVoyager(
    path: String,
    m: Int,
    ef: Int,
    voyagerDistanceMeasure: VoyagerDistanceMeasure
  ): String =
    ""

  def asVoyagerDistCache(): DistCache[VoyagerReader] = ???

//  private class VoyagerDistCache(
//    val view: PCollectionView[VoyagerUri],
//    distanceMeasure: VoyagerDistanceMeasure,
//    dim: Int
//  ) extends DistCache[VoyagerReader] {
//    override def apply(): VoyagerReader = ???
//  }

}
