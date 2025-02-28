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

package com.spotify.scio.values

import com.spotify.scio._
import com.spotify.scio.io._
import com.spotify.scio.testing._
import com.spotify.sparkey.SparkeyReader.Entry
import com.spotify.sparkey.{IndexHeader, LogHeader, Sparkey, SparkeyReader}
import org.typelevel.scalaccompat.annotation.unused

import scala.io.Source

// =======================================================================
// Test jobs
// =======================================================================

object SimpleDistCacheJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val dc =
      sc.distCache(args("distCache"))(f => Source.fromFile(f).getLines().toSeq)
    sc.textFile(args("input"))
      .flatMap(x => dc().map(x + _))
      .saveAsTextFile(args("output"))
    sc.run()
    ()
  }
}

class NonSerializable(val noDefaultCntr: String) extends Serializable {
  // make sure it's not kryo/java serializable
  @unused private val t = new Thread()
}

object NonSerializableDistCacheJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val dc = sc.distCache(args("distCache"))(_ => new NonSerializable("foobar"))
    sc.textFile(args("input"))
      .map(_ => dc().noDefaultCntr)
      .saveAsTextFile(args("output"))
    sc.run()
    ()
  }
}

object SparkeyDistCacheJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val dc = sc.distCache(Seq(".spi", ".spl").map(args("sparkey") + _))(fs => Sparkey.open(fs.head))
    sc.textFile(args("input"))
      .map(dc().getAsString)
      .saveAsTextFile(args("output"))
    sc.run()
    ()
  }
}

// =======================================================================
// Test transforms
// =======================================================================

object DistCacheTest {
  def simpleTransform(in: SCollection[String], dc: DistCache[Seq[String]]): SCollection[String] =
    in.flatMap(x => dc().map(x + _))

  def sparkeyTransform(in: SCollection[String], dc: DistCache[SparkeyReader]): SCollection[String] =
    in.map(dc().getAsString)
}

// =======================================================================
// Tests
// =======================================================================

class DistCacheTest extends PipelineSpec {
  import DistCacheTest._

  // =======================================================================
  // Plain text
  // =======================================================================

  "DistCache" should "work with JobTest" in {
    JobTest[SimpleDistCacheJob.type]
      .args("--input=in.txt", "--output=out.txt", "--distCache=dc.txt")
      .input(TextIO("in.txt"), Seq("a", "b"))
      .distCache(DistCacheIO("dc.txt"), Seq("1", "2"))
      .output(TextIO("out.txt")) { coll =>
        coll should containInAnyOrder(Seq("a1", "a2", "b1", "b2"))
      }
      .run()
  }

  it should "work with runWithContext" in {
    val dc = MockDistCache(Seq("1", "2"))
    runWithContext { sc =>
      val in = sc.parallelize(Seq("a", "b"))
      simpleTransform(in, dc) should containInAnyOrder(Seq("a1", "a2", "b1", "b2"))
    }
  }

  it should "work with runWithData" in {
    val dc = MockDistCache(Seq("1", "2"))
    runWithData(Seq("a", "b")) {
      _.flatMap(x => dc().map(x + _))
    } should contain theSameElementsAs Seq("a1", "a2", "b1", "b2")
  }

  it should "work with runWithData and non-serializable dist cache" in {
    val dc = MockDistCache(() => new NonSerializable("foobar"))
    runWithData(Seq("a", "b")) {
      _.map(_ => dc().noDefaultCntr)
    } should contain theSameElementsAs Seq("foobar", "foobar")
  }

  it should "work for non-serializable dist cache" in {
    JobTest[NonSerializableDistCacheJob.type]
      .args("--input=in.txt", "--output=out.txt", "--distCache=dc.txt")
      .input(TextIO("in.txt"), Seq("a", "b"))
      .distCacheFunc(DistCacheIO("dc.txt"), () => new NonSerializable("foobar"))
      .output(TextIO("out.txt"))(coll => coll should containInAnyOrder(Seq("foobar", "foobar")))
      .run()
  }

  // =======================================================================
  // Sparkey
  // =======================================================================

  val sparkey: SparkeyReader =
    new MockSparkeyReader(Map("a" -> "alpha", "b" -> "bravo", "c" -> "charlie"))

  "Sparkey" should "work with JobTest" in {
    JobTest[SparkeyDistCacheJob.type]
      .args("--input=in.txt", "--output=out.txt", "--sparkey=data.sparkey")
      .input(TextIO("in.txt"), Seq("a", "b"))
      .distCache(DistCacheIO(Seq("data.sparkey.spi", "data.sparkey.spl")), sparkey)
      .output(TextIO("out.txt"))(coll => coll should containInAnyOrder(Seq("alpha", "bravo")))
      .run()
  }

  it should "work with runWithContext" in {
    val dc = MockDistCache(sparkey)
    runWithContext { sc =>
      val in = sc.parallelize(Seq("a", "b"))
      sparkeyTransform(in, dc) should containInAnyOrder(Seq("alpha", "bravo"))
    }
  }

  it should "work with runWithData" in {
    val dc = MockDistCache(sparkey)
    runWithData(Seq("a", "b")) {
      _.map(dc().getAsString)
    } should contain theSameElementsAs Seq("alpha", "bravo")
  }
}

// =======================================================================
// Mocks
// =======================================================================

// Mock SparkeyReader with fake data and serializable
class MockSparkeyReader(private val data: Map[String, String])
    extends SparkeyReader
    with Serializable {
  override def getLogHeader: LogHeader = ???
  override def getAsString(key: String): String = data(key)
  override def getAsEntry(key: Array[Byte]): Entry = ???
  override def iterator(): java.util.Iterator[Entry] = ???
  override def close(): Unit = ???
  override def getAsByteArray(key: Array[Byte]): Array[Byte] = ???
  override def getIndexHeader: IndexHeader = ???
  override def duplicate(): SparkeyReader = ???
  override def getLoadedBytes: Long = ???
  override def getTotalBytes: Long = ???
}
