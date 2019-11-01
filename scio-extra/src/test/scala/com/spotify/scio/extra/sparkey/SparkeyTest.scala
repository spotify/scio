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

import com.spotify.scio.util._
import com.github.benmanes.caffeine.cache.{Caffeine, Cache => CCache}
import com.spotify.scio._
import com.spotify.scio.testing._
import com.spotify.sparkey._
import org.apache.beam.sdk.io.FileSystems
import org.apache.commons.io.FileUtils

import scala.collection.JavaConverters._

final case class TestCache[K, V](testId: String) extends CacheT[K, V, CCache[K, V]] {
  @transient private lazy val cache =
    TestCache.caches
      .get(testId, Cache.caffeine(Caffeine.newBuilder().recordStats().build()))
      .asInstanceOf[CacheT[K, V, CCache[K, V]]]

  override def get(k: K): Option[V] = cache.get(k)

  override def get(k: K, default: => V): V = cache.get(k, default)

  override def put(k: K, value: V): Unit = {
    cache.put(k, value)
    ()
  }

  override def invalidateAll(): Unit = cache.invalidateAll()

  override def underlying: CCache[K, V] = cache.underlying
}

object TestCache {
  private val caches = Cache.concurrentHashMap[String, AnyRef]

  @inline def apply[K, V](): TestCache[K, V] =
    TestCache[K, V](scala.util.Random.nextString(5))
}

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

  "SCollection" should "support reading in an existing Sparkey file" in {
    // Create a temporary Sparkey file pair
    val sc = ScioContext()
    val p = sc.parallelize(sideData).asSparkey.materialize
    val scioResult = sc.run().waitUntilFinish()
    val basePath = scioResult.tap(p).value.next().basePath

    val sc2 = ScioContext()
    val sparkey = sc2.sparkeySideInput(basePath)
    val contents = sc2
      .parallelize(Seq(1))
      .withSideInputs(sparkey)
      .flatMap { case (_, sic) => sic(sparkey).toList }
      .toSCollection
      .materialize
    val valueResult = sc2.run().waitUntilFinish()

    valueResult.tap(contents).value.toSet shouldBe sideData.toSet

    for (ext <- Seq(".spi", ".spl")) {
      new File(basePath + ext).delete()
    }
  }

  "SCollection" should "support reading in an existing sharded Sparkey collection" in {
    // Create a temporary Sparkey file pair
    val sc = ScioContext()
    val p = sc.parallelize(sideData).asSparkey(numShards = 2).materialize
    val scioResult = sc.run().waitUntilFinish()
    val sparkeyUri = scioResult.tap(p).value.next().asInstanceOf[ShardedSparkeyUri]
    val globExpression = sparkeyUri.globExpression

    val sc2 = ScioContext()
    val sparkey = sc2.sparkeySideInput(globExpression)
    val contents = sc2
      .parallelize(Seq(1))
      .withSideInputs(sparkey)
      .flatMap { case (_, sic) => sic(sparkey).toList }
      .toSCollection
      .materialize
    val valueResult = sc2.run().waitUntilFinish()

    valueResult.tap(contents).value.toSet shouldBe sideData.toSet

    FileUtils.deleteDirectory(new File(sparkeyUri.basePath))
  }

  "SCollection" should "support .asSparkey with shards" in {
    val sc = ScioContext()
    val p = sc.parallelize(sideData).asSparkey(numShards = 2).materialize
    val scioResult = sc.run().waitUntilFinish()

    val sparkeyUri = scioResult.tap(p).value.next().asInstanceOf[LocalShardedSparkeyUri]

    val allSparkeyFiles = FileSystems
      .`match`(sparkeyUri.globExpression)
      .metadata
      .asScala
      .map(_.resourceId.toString)

    val basePaths = allSparkeyFiles.map(_.replaceAll("\\.sp[il]$", "")).toSet

    val readers = basePaths.map(basePath => Sparkey.open(new File(basePath)))
    readers.map(_.toMap.toList.toMap).reduce(_ ++ _) shouldBe sideData.toMap

    FileUtils.deleteDirectory(new File(sparkeyUri.basePath))
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

  it should "support .asSparkeySideInput with shards" in {
    val sc = ScioContext()

    val input = Seq("a", "b", "a", "b")

    val sparkey = sc.parallelize(sideData).asSparkey(numShards = 2)
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
    FileUtils.deleteDirectory(new File(basePath))
  }

  it should "support .asCachedStringSparkeySideInput" in {
    val sc = ScioContext()

    val input = Seq("a", "b", "a", "b")

    val cache = TestCache[String, String]()

    val sparkey = sc.parallelize(sideData).asSparkey
    val sparkeyMaterialized = sparkey.materialize
    val si = sparkey.asCachedStringSparkeySideInput(cache)

    val result = sc
      .parallelize(input)
      .withSideInputs(si)
      .map((x, sic) => sic(si)(x))
      .toSCollection
      .materialize

    val scioResult = sc.run().waitUntilFinish()

    scioResult.tap(result).value.toList.sorted shouldBe input.map(sideData.toMap).sorted

    cache.underlying.stats().requestCount shouldBe input.size
    cache.underlying.stats().loadCount shouldBe input.toSet.size

    val basePath = scioResult.tap(sparkeyMaterialized).value.next().basePath
    for (ext <- Seq(".spi", ".spl")) new File(basePath + ext).delete()
  }

  it should "support .asCachedStringSparkeySideInput with shards" in {
    val sc = ScioContext()

    val input = Seq("a", "b", "a", "b")

    val cache = TestCache[String, String]()

    val sparkey = sc.parallelize(sideData).asSparkey(numShards = 2)
    val sparkeyMaterialized = sparkey.materialize
    val si = sparkey.asCachedStringSparkeySideInput(cache)

    val result = sc
      .parallelize(input)
      .withSideInputs(si)
      .map((x, sic) => sic(si)(x))
      .toSCollection
      .materialize

    val scioResult = sc.run().waitUntilFinish()

    scioResult.tap(result).value.toList.sorted shouldBe input.map(sideData.toMap).sorted

    cache.underlying.stats().requestCount shouldBe input.size
    cache.underlying.stats().loadCount shouldBe input.toSet.size

    val basePath = scioResult.tap(sparkeyMaterialized).value.next().basePath
    FileUtils.deleteDirectory(new File(basePath))
  }

  it should "support .asTypedSparkeySideInput" in {
    val sc = ScioContext()

    val input = Seq("a", "b", "c", "d")
    val typedSideData = Seq(("a", Seq(1, 2)), ("b", Seq(2, 3)), ("c", Seq(3, 4)))
    val typedSideDataMap = typedSideData.toMap

    val sparkey = sc.parallelize(typedSideData).mapValues(_.map(_.toString).mkString(",")).asSparkey
    val sparkeyMaterialized = sparkey.materialize

    val si = sparkey.asTypedSparkeySideInput[Seq[Int]] { b: Array[Byte] =>
      new String(b).split(",").toSeq.map(_.toInt)
    }

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

    val cache = TestCache[String, AnyRef]()

    val sparkey = sc.parallelize(typedSideData).mapValues(_.mkString(",")).asSparkey
    val sparkeyMaterialized = sparkey.materialize

    val si = sparkey.asTypedSparkeySideInput[Object](cache) { b: Array[Byte] =>
      new String(b).split(",").map(_.toInt).toSeq
    }

    val result = sc
      .parallelize(input)
      .withSideInputs(si)
      .flatMap((x, sic) => sic(si).get(x))
      .toSCollection
      .materialize

    val scioResult = sc.run().waitUntilFinish()
    val expectedOutput = input.flatMap(typedSideDataMap.get)

    scioResult.tap(result).value.toList should contain theSameElementsAs expectedOutput

    cache.underlying.stats().requestCount shouldBe input.size
    cache.underlying.stats().loadCount shouldBe input.toSet.size

    val basePath = scioResult.tap(sparkeyMaterialized).value.next().basePath
    for (ext <- Seq(".spi", ".spl")) new File(basePath + ext).delete()
  }

  it should "support iteration with .asTypedSparkeySideInput" in {
    val sc = ScioContext()

    val input = Seq("1")
    val typedSideData = Seq(("a", Seq(1, 2)), ("b", Seq(2, 3)), ("c", Seq(3, 4)))

    val sparkey = sc.parallelize(typedSideData).mapValues(_.map(_.toString).mkString(",")).asSparkey
    val sparkeyMaterialized = sparkey.materialize

    val si = sparkey.asTypedSparkeySideInput[Seq[Int]] { b: Array[Byte] =>
      new String(b).split(",").toSeq.map(_.toInt)
    }

    val result = sc
      .parallelize(input)
      .withSideInputs(si)
      .flatMap((_, sic) => sic(si).iterator.toList.sortBy(_._1).map(_._2))
      .toSCollection
      .materialize

    val scioResult = sc.run().waitUntilFinish()
    val expectedOutput = typedSideData.map(_._2)

    scioResult.tap(result).value.toList should contain theSameElementsAs expectedOutput

    val basePath = scioResult.tap(sparkeyMaterialized).value.next().basePath
    for (ext <- Seq(".spi", ".spl")) new File(basePath + ext).delete()
  }

  it should "support iteration .asTypedSparkeySideInput with a cache" in {
    val sc = ScioContext()

    val input = Seq("1")
    val typedSideData = Seq(("a", Seq(1, 2)), ("b", Seq(2, 3)), ("c", Seq(3, 4)))

    val cache = TestCache[String, AnyRef]()

    val sparkey = sc.parallelize(typedSideData).mapValues(_.mkString(",")).asSparkey
    val sparkeyMaterialized = sparkey.materialize

    val si = sparkey.asTypedSparkeySideInput[Object](cache) { b: Array[Byte] =>
      new String(b).split(",").map(_.toInt).toSeq
    }

    val result = sc
      .parallelize(input)
      .withSideInputs(si)
      .flatMap((_, sic) => sic(si).iterator.toList.sortBy(_._1).map(_._2))
      .toSCollection
      .materialize

    val scioResult = sc.run().waitUntilFinish()
    val expectedOutput = typedSideData.map(_._2)

    scioResult.tap(result).value.toList should contain theSameElementsAs expectedOutput

    cache.underlying.stats().requestCount shouldBe typedSideData.size
    cache.underlying.stats().loadCount shouldBe 0

    val basePath = scioResult.tap(sparkeyMaterialized).value.next().basePath
    for (ext <- Seq(".spi", ".spl")) new File(basePath + ext).delete()
  }

  it should "support iteration .asTypedSparkeySideInput with a cache and shards" in {
    val sc = ScioContext()

    val input = Seq("1")
    val typedSideData = Seq(("a", Seq(1, 2)), ("b", Seq(2, 3)), ("c", Seq(3, 4)))

    val cache = TestCache[String, AnyRef]()

    val sparkey = sc.parallelize(typedSideData).mapValues(_.mkString(",")).asSparkey(numShards = 2)
    val sparkeyMaterialized = sparkey.materialize

    val si = sparkey.asTypedSparkeySideInput[Object](cache) { b: Array[Byte] =>
      new String(b).split(",").map(_.toInt).toSeq
    }

    val result = sc
      .parallelize(input)
      .withSideInputs(si)
      .flatMap((_, sic) => sic(si).iterator.toList.sortBy(_._1).map(_._2))
      .toSCollection
      .materialize

    val scioResult = sc.run().waitUntilFinish()
    val expectedOutput = typedSideData.map(_._2)

    scioResult.tap(result).value.toList should contain theSameElementsAs expectedOutput

    cache.underlying.stats().requestCount shouldBe typedSideData.size
    cache.underlying.stats().loadCount shouldBe 0

    val basePath = scioResult.tap(sparkeyMaterialized).value.next().basePath
    FileUtils.deleteDirectory(new File(basePath))
  }
}
