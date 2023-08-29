package com.spotify.scio.extra.voyager

import com.spotify.scio.{ScioContext, ScioResult}
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
    Seq(("1", Array(2.5f, 7.2f)), ("2", Array(1.2f, 2.2f)), ("3", Array(5.6f, 3.4f)))

  "SCollection" should "support .asVoyager with specified local file" in {
    val tmpDir = Files.createTempDirectory("voyager-test")
    val basePath = tmpDir.toString
    val sc = ScioContext()
    val p: ClosedTap[VoyagerUri] =
      sc.parallelize(sideData)
        .asVoyager(basePath, distanceMeasure, storageType, dim)
        .materialize

    val scioResult: ScioResult = sc.run().waitUntilFinish()
    val path: String = scioResult.tap(p).value.next.path

    val index: StringIndex = StringIndex.load(
      path + "/index.hnsw",
      path + "/names.json",
      SpaceType.Cosine,
      dim,
      StorageDataType.E4M3
    )

    sideData.foreach { data =>
      val result = index.query(data._2, 2, 100)
      result.getNames.length shouldEqual 2
      result.getDistances.length shouldEqual 2
      result.getNames should contain(data._1)
    }

    for (file <- Seq("index.hnsw", "names.json")) {
      new File(basePath + file).delete()
    }
  }

  it should "throw exception when the Voyager files already exists" in {
    val tmpDir = Files.createTempDirectory("voyager-test")

    val index = tmpDir.resolve("index.hnsw")
    val names = tmpDir.resolve("names.json")
    Files.createFile(index)
    Files.createFile(names)

    the[IllegalArgumentException] thrownBy {
      runWithContext {
        _.parallelize(sideData).asVoyager(tmpDir.toString, Cosine, E4M3, dim)
      }
    } should have message s"requirement failed: Voyager URI $tmpDir already exists"

    Files.delete(index)
    Files.delete(names)
  }

}
