package com.spotify.scio.extra.voyager

import com.spotify.scio.ScioContext
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.testing.PipelineSpec
import com.spotify.voyager.jni.Index.{SpaceType, StorageDataType}
import com.spotify.voyager.jni.StringIndex

import java.io.File
import java.nio.file.Files

class VoyagerTest extends PipelineSpec {
  val distanceMeasure = Cosine
  val storageType = E4M3
  val dim = 2

  val sideData: Seq[(String, Array[Float])] =
    Seq(("1", Array(1.1f, 1.1f)), ("2", Array(2.2f, 2.2f)), ("3", Array(3.3f, 3.3f)))

  "SCollection" should "support .asVoyager with specified local file" in {
    val tmpDir = Files.createTempDirectory("voyager-test")
    val basePath = tmpDir.toString
    val sc = ScioContext()
    val p: ClosedTap[VoyagerUri] =
      sc.parallelize(sideData)
        .asVoyager(basePath, distanceMeasure, storageType, dim)
        .materialize

    val scioResult = sc.run().waitUntilFinish()
    val path = scioResult.tap(p).value.next.path
    println(path)


    val reader = StringIndex.load(
      path + "/index.hnsw",
      path + "/names.json",
      SpaceType.Cosine,
      dim,
      StorageDataType.E4M3
    )

    sideData.foreach { s =>
      println(reader.query(s._2, 1, 1).getNames.toString)
    }

    for (file <- Seq("index.hnsw", "names.json")) {
      new File(basePath + file).delete()
    }

  }

}
