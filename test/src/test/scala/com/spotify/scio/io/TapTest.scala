package com.spotify.scio.io

import java.io.File
import java.util.UUID

import com.spotify.scio.ScioContext
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.testing.TestingUtils._
import org.apache.commons.io.FileUtils

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class TapTest extends PipelineSpec {

  def makeRecords(sc: ScioContext) =
    sc.parallelize(Seq(1, 2, 3))
      .map(i => (newSpecificRecord(i), newGenericRecord(i)))

  def expectedRecords = Set(1, 2, 3).map(i => (newSpecificRecord(i), newGenericRecord(i)))

  "Future" should "support saveAsInMemoryTap" in {
    runWithInMemoryFuture {
      makeRecords(_).saveAsInMemoryTap
    }.toSet should equal (expectedRecords)
  }

  it should "update isCompleted with testId" in {
    val testId = "FutureTest-" + System.currentTimeMillis()
    val sc = ScioContext(Array(s"--testId=$testId"))
    val f = sc.parallelize(Seq(1, 2, 3))
      .map(newSpecificRecord)
      .saveAsInMemoryTap
    f.isCompleted shouldBe false
    sc.close()
    f.isCompleted shouldBe true
  }

  it should "update isCompleted without testId" in {
    val dir = tmpDir
    val sc = ScioContext()
    val f = sc.parallelize(Seq(1, 2, 3))
      .map(newSpecificRecord)
      .saveAsAvroFile(dir.getPath)
    f.isCompleted shouldBe false
    sc.close()
    f.isCompleted shouldBe true
    FileUtils.deleteDirectory(dir)
  }

  it should "support materialize" in {
    runWithInMemoryFuture {
      makeRecords(_).materialize
    }.toSet should equal (expectedRecords)

    runWithFileFuture {
      makeRecords(_).materialize
    }.toSet should equal (expectedRecords)
  }

  it should "support saveAsAvroFile with SpecificRecord" in {
    val dir = tmpDir
    runWithFileFuture {
      _
        .parallelize(Seq(1, 2, 3))
        .map(newSpecificRecord)
        .saveAsAvroFile(dir.getPath)
    }.toSet should equal (Set(1, 2, 3).map(newSpecificRecord))
    FileUtils.deleteDirectory(dir)
  }

  it should "support saveAsAvroFile with GenericRecord" in {
    val dir = tmpDir
    runWithFileFuture {
      _
        .parallelize(Seq(1, 2, 3))
        .map(newGenericRecord)
        .saveAsAvroFile(dir.getPath, schema = newGenericRecord(1).getSchema)
    }.toSet should equal (Set(1, 2, 3).map(newGenericRecord))
    FileUtils.deleteDirectory(dir)
  }

  it should "support saveAsTableRowJsonFile" in {
    val dir = tmpDir
    // Compare .toString versions since TableRow may not round trip
    runWithFileFuture {
      _
        .parallelize(Seq(1, 2, 3))
        .map(newTableRow)
        .saveAsTableRowJsonFile(dir.getPath)
    }.map(_.toString).toSet should equal (Set(1, 2, 3).map(i => newTableRow(i).toString))
    FileUtils.deleteDirectory(dir)
  }

  it should "support saveAsTextFile" in {
    val dir = tmpDir
    runWithFileFuture {
      _
        .parallelize(Seq(1, 2, 3))
        .map(i => newTableRow(i).toString)
        .saveAsTextFile(dir.getPath)
    }.toSet should equal (Set(1, 2, 3).map(i => newTableRow(i).toString))
    FileUtils.deleteDirectory(dir)
  }

  def runWithInMemoryFuture[T](fn: ScioContext => Future[Tap[T]]): Iterator[T] =
    runWithFuture(Array(s"--testId=FutureTest-" + System.currentTimeMillis()))(fn)

  def runWithFileFuture[T](fn: ScioContext => Future[Tap[T]]): Iterator[T] =
    runWithFuture(Array.empty)(fn)

  def runWithFuture[T](args: Array[String])(fn: ScioContext => Future[Tap[T]]): Iterator[T] = {
    val sc = ScioContext(args)
    val f = fn(sc)
    sc.close()
    Await.result(f, Duration.Inf).value
  }

  def tmpDir = new File(new File(sys.props("java.io.tmpdir")), "scio-test-" + UUID.randomUUID().toString)

}
