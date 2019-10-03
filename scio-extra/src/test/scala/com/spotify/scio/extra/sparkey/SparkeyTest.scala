/*
 * Copyright 2019 Spotify AB.
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

package com.spotify.scio.extra.sparkey

import java.io.File
import java.nio.file.Files
import java.util.Arrays

import com.spotify.scio._
import com.spotify.scio.testing._
import com.spotify.sparkey._

class SparkeyTest extends PipelineSpec {

  val sideData = Seq(("a", "1"), ("b", "2"), ("c", "3"))

  "SCollection" should "support .asSparkey with temporary local file" in {
    val sc = ScioContext()
    val p = sc.parallelize(sideData).asSparkey.materialize
    val scioResult = sc.run().waitUntilFinish()
    val basePath = scioResult.tap(p).value.next().basePath
    val reader = Sparkey.open(new File(basePath))
    reader.toMap shouldBe sideData.toMap
    for (ext <- Seq(".spi", ".spl")) {
      new File(basePath + ext).delete()
    }
  }

  it should "support .asSparkey with specified local file" in {
    val tmpDir = Files.createTempDirectory("sparkey-test-")
    val basePath = tmpDir.resolve("sparkey").toString
    runWithContext { sc =>
      sc.parallelize(sideData).asSparkey(basePath)
    }
    val reader = Sparkey.open(new File(basePath + ".spi"))
    reader.toMap shouldBe sideData.toMap
    for (ext <- Seq(".spi", ".spl")) {
      new File(basePath + ext).delete()
    }
  }

  it should "throw exception when Sparkey file exists" in {
    val tmpDir = Files.createTempDirectory("sparkey-test-")
    val basePath = tmpDir.resolve("sparkey").toString
    val index = new File(basePath + ".spi")
    Files.createFile(index.toPath)
    // scalastyle:off no.whitespace.before.left.bracket
    the[IllegalArgumentException] thrownBy {
      runWithContext {
        _.parallelize(sideData).asSparkey(basePath)
      }
    } should have message s"requirement failed: Sparkey URI $basePath already exists"
    // scalastyle:on no.whitespace.before.left.bracket
    index.delete()
  }

  it should "support .asSparkey with Array[Byte] key, value" in {
    val tmpDir = Files.createTempDirectory("sparkey-test-")
    val sideDataBytes = sideData.map { kv =>
      (kv._1.getBytes, kv._2.getBytes)
    }
    val basePath = tmpDir + "/my-sparkey-file"
    runWithContext { sc =>
      sc.parallelize(sideDataBytes).asSparkey(basePath)
    }
    val reader = Sparkey.open(new File(basePath + ".spi"))
    sideDataBytes.foreach { kv =>
      Arrays.equals(reader.getAsByteArray(kv._1), kv._2) shouldBe true
    }
    for (ext <- Seq(".spi", ".spl")) {
      new File(basePath + ext).delete()
    }
  }

  it should "support .asSparkeySideInput" in {
    val sc = ScioContext()

    val input = Seq("a", "b", "a", "b")

    val sparkey = sc.parallelize(sideData).asSparkey
    val sparkeyMaterialized = sparkey.materialize
    val si = sparkey.asSparkeySideInput
    val result = sc
      .parallelize(input)
      .withSideInputs(si)
      .map((x, sic) => sic(si)(x))
      .toSCollection
      .materialize

    val scioResult = sc.run().waitUntilFinish()

    scioResult.tap(result).value.toList.sorted shouldBe input.map(sideData.toMap).sorted

    val basePath = scioResult.tap(sparkeyMaterialized).value.next().basePath
    for (ext <- Seq(".spi", ".spl")) new File(basePath + ext).delete()
  }

  it should "support .asTypedSparkeySideInput" in {
    val sc = ScioContext()

    val input = Seq("a", "b", "c", "d")
    val typedSideData = Seq(("a", Seq(1, 2)), ("b", Seq(2, 3)), ("c", Seq(3, 4)))
    val typedSideDataMap = typedSideData.toMap

    val sparkey = sc.parallelize(typedSideData).mapValues(_.map(_.toString).mkString(",")).asSparkey
    val sparkeyMaterialized = sparkey.materialize

    val si = sparkey.asTypedSparkeySideInput[Seq[Int]](new String(_).split(",").toSeq.map(_.toInt))

    val result = sc
      .parallelize(input)
      .withSideInputs(si)
      .flatMap((x, sic) => sic(si).get(x))
      .toSCollection
      .materialize

    val scioResult = sc.run().waitUntilFinish()
    val expectedOutput = input.flatMap(typedSideDataMap.get)

    scioResult.tap(result).value.toList should contain theSameElementsAs expectedOutput

    val basePath = scioResult.tap(sparkeyMaterialized).value.next().basePath
    for (ext <- Seq(".spi", ".spl")) new File(basePath + ext).delete()
  }

  it should "support .asTypedSparkeySideInput with a cache" in {
    val sc = ScioContext()

    val input = Seq("a", "b", "c", "d")
    val typedSideData = Seq(("a", Seq(1, 2)), ("b", Seq(2, 3)), ("c", Seq(3, 4)))
    val typedSideDataMap = typedSideData.toMap

    MockCache.reset()

    val sparkey = sc.parallelize(typedSideData).mapValues(_.mkString(",")).asSparkey
    val sparkeyMaterialized = sparkey.materialize

    val si = sparkey.asTypedSparkeySideInput[Object](
      bytes => new String(bytes).split(",").map(_.toInt).toSeq,
      MockCache.getInstance
    )

    val result = sc
      .parallelize(input)
      .withSideInputs(si)
      .flatMap((x, sic) => sic(si).get(x))
      .toSCollection
      .materialize

    val scioResult = sc.run().waitUntilFinish()
    val expectedOutput = input.flatMap(typedSideDataMap.get)

    scioResult.tap(result).value.toList should contain theSameElementsAs expectedOutput

    MockCache.getStats.requestCount shouldBe input.size
    MockCache.getStats.loadCount shouldBe input.toSet.size

    val basePath = scioResult.tap(sparkeyMaterialized).value.next().basePath
    for (ext <- Seq(".spi", ".spl")) new File(basePath + ext).delete()
  }

}
