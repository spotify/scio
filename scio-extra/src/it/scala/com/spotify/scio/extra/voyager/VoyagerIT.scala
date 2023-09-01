package com.spotify.scio.extra.voyager

import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.testing.util.ItUtils
import com.spotify.scio.values.{SCollection, SideInput}
import org.apache.beam.sdk.io.FileSystems

import scala.jdk.CollectionConverters._
class VoyagerIT extends PipelineSpec {
  val dim: Int = 2
  val storageType: VoyagerStorageType = E4M3
  val distanceMeasure: VoyagerDistanceMeasure = Cosine

  val sideData: Seq[(String, Array[Float])] =
    Seq(("1", Array(2.5f, 7.2f)), ("2", Array(1.2f, 2.2f)), ("3", Array(5.6f, 3.4f)))
  it should "support .asVoyagerSideInput using GCS tempLocation" in {
    runWithContext { sc =>
      FileSystems.setDefaultPipelineOptions(sc.options)

      val tempLocation = ItUtils.gcpTempLocation("voyager-it")

      try {
        val p1 = sc.parallelize(sideData)
        val p2: SideInput[VoyagerReader] =
          sc.parallelize(sideData).asVoyagerSideInput(distanceMeasure, storageType, dim)
        val s = p1
          .withSideInputs(p2)
          .flatMap { (xs, si) =>
            si(p2).getNearest(xs._2, 1, 100)
          }
          .toSCollection

        s should containInAnyOrder(sideData.map(_._1))
      } finally {
        val files = FileSystems
          .`match`(s"$tempLocation")
          .metadata()
          .asScala
          .map(_.resourceId())

        FileSystems.delete(files.asJava)
      }
    }
  }
}
