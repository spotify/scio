package com.spotify.scio.sinks

import java.io.File
import java.util.UUID

import com.spotify.scio.ScioContext
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.testing.TestingUtils._
import org.apache.commons.io.FileUtils

class SinkTest extends PipelineSpec {

  def makeRecords(sc: ScioContext) =
    sc.parallelize(Seq(1, 2, 3))
      .map(i => (newSpecificRecord(i), newGenericRecord(i)))

  def expectedRecords = Set(1, 2, 3).map(i => (newSpecificRecord(i), newGenericRecord(i)))

  "Sink" should "support saveAsInMemorySink" in {
    runWithInMemorySink {
      makeRecords(_).saveAsInMemorySink
    }.value.toSet should equal (expectedRecords)
  }

  it should "support materialize" in {
    runWithInMemorySink {
      makeRecords(_).materialize
    }.value.toSet should equal (expectedRecords)

    runWithFileSink {
      makeRecords(_).materialize
    }.value.toSet should equal (expectedRecords)
  }

  it should "support saveAsAvroFile with SpecificRecord" in {
    val dir = tmpDir
    runWithFileSink {
      _
        .parallelize(Seq(1, 2, 3))
        .map(newSpecificRecord)
        .saveAsAvroFile(dir.getPath)
    }.value.toSet should equal (Set(1, 2, 3).map(newSpecificRecord))
    FileUtils.deleteDirectory(dir)
  }

  it should "support saveAsAvroFile with GenericRecord" in {
    val dir = tmpDir
    runWithFileSink {
      _
        .parallelize(Seq(1, 2, 3))
        .map(newGenericRecord)
        .saveAsAvroFile(dir.getPath, schema = newGenericRecord(1).getSchema)
    }.value.toSet should equal (Set(1, 2, 3).map(newGenericRecord))
    FileUtils.deleteDirectory(dir)
  }

  it should "support saveAsTableRowJsonFile" in {
    val dir = tmpDir
    // Compare .toString versions since TableRow may not round trip
    runWithFileSink {
      _
        .parallelize(Seq(1, 2, 3))
        .map(newTableRow)
        .saveAsTableRowJsonFile(dir.getPath)
    }.value.map(_.toString).toSet should equal (Set(1, 2, 3).map(i => newTableRow(i).toString))
    FileUtils.deleteDirectory(dir)
  }

  it should "support saveAsTextFile" in {
    val dir = tmpDir
    runWithFileSink {
      _
        .parallelize(Seq(1, 2, 3))
        .map(i => newTableRow(i).toString)
        .saveAsTextFile(dir.getPath)
    }.value.toSet should equal (Set(1, 2, 3).map(i => newTableRow(i).toString))
    FileUtils.deleteDirectory(dir)
  }

  it should "throw exception if context is not closed" in {
    intercept[RuntimeException] {
      val testId = "SinkTest-" + System.currentTimeMillis()
      val sc = ScioContext(Array(s"--testId=$testId"))
      sc.parallelize(Seq(1, 2, 3))
        .map(newSpecificRecord)
        .saveAsInMemorySink
        .value
    }

    intercept[RuntimeException] {
      val sc = ScioContext(Array.empty)
      sc.parallelize(Seq(1, 2, 3))
        .map(newSpecificRecord)
        .saveAsAvroFile(tmpDir.getPath)
        .value
    }
  }

  def runWithInMemorySink[T](fn: ScioContext => Sink[T]): Sink[T] = {
    val testId = "SinkTest-" + System.currentTimeMillis()
    val sc = ScioContext(Array(s"--testId=$testId"))
    val sink = fn(sc)
    sc.close()
    sink
  }

  def runWithFileSink[T](fn: ScioContext => Sink[T]): Sink[T] = {
    val sc = ScioContext(Array.empty)
    val sink = fn(sc)
    sc.close()
    sink
  }

  def runWithSink[T](args: Array[String])(fn: ScioContext => Sink[T]): Sink[T] = {
    val sc = ScioContext(args)
    val sink = fn(sc)
    sc.close()
    sink
  }

  def tmpDir = new File(new File(sys.props("java.io.tmpdir")), "scio-test-" + UUID.randomUUID().toString)

}
