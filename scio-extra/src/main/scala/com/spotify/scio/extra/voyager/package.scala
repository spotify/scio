package com.spotify.scio.extra

import com.spotify.scio.ScioContext
import com.spotify.scio.annotations.experimental
import com.spotify.scio.values.{DistCache, SCollection, SideInput}
import com.spotify.voyager.jni.Index.{SpaceType, StorageDataType}
import com.spotify.voyager.jni.{Index, StringIndex}
import org.apache.beam.sdk.transforms.{DoFn, View}
import org.apache.beam.sdk.values.PCollectionView

import java.nio.file.{Path, Paths}

package object voyager {
  sealed abstract class VoyagerDistanceMeasure
  case object Euclidean extends VoyagerDistanceMeasure
  case object Cosine extends VoyagerDistanceMeasure
  case object Dot extends VoyagerDistanceMeasure

  sealed abstract class VoyagerStorageType
  case object Float8 extends VoyagerStorageType
  case object Float32 extends VoyagerStorageType
  case object E4M3 extends VoyagerStorageType

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
              voyagerWriter.save(path)
              uri
            }
        }
      }
    }

  }

  def asVoyagerSideInput(): SideInput[VoyagerReader] = ???

  /**
   * To be used with with side inputs
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
