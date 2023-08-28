package com.spotify.scio.extra

import com.spotify.scio.annotations.experimental
import com.spotify.scio.values.{DistCache, SCollection}
import com.spotify.voyager.jni.Index.{SpaceType, StorageDataType}
import com.spotify.voyager.jni.{Index, StringIndex}

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

    private val indexPath: String = path + "/index.hnsw"
    private val namesPath: String = path + "/names.json"

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
      StringIndex.load(indexPath, namesPath, spaceType, dim, storageDataType)
    }
    // figure out index path + names path

//    def getNearest(v: Array[Float], maxNumResults: Int) = index.query(v, maxNumResults)
//
//    def getItemVector(i: Int) = index.getVector(i)
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
      ef: Long = 200L,
      m: Long = 16L
    ): SCollection[VoyagerUri] = {
      val uri: VoyagerUri = VoyagerUri(path, self.context.options)
      require(!uri.exists, s"Voyager URI ${uri.path} already exists")
      self.transform { in =>
        {
          in.groupBy(_ => ())
            .map { case (_, xs) =>
              val voyagerWriter: VoyagerWriter =
                new VoyagerWriter(voyagerDistanceMeasure, voyagerStorageType, dim, ef, m)
              voyagerWriter.write(xs)
              voyagerWriter.save("index.hnsw", "names.json")
              uri
            }
        }
      }
    }

  }

  def asVoyagerDistCache(): DistCache[VoyagerReader] = ???

//  private class VoyagerDistCache(
//    val view: PCollectionView[VoyagerUri],
//    distanceMeasure: VoyagerDistanceMeasure,
//    dim: Int
//  ) extends DistCache[VoyagerReader] {
//    override def apply(): VoyagerReader = ???
//  }

}
