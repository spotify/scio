package com.spotify.scio.avro

import com.spotify.scio.io.ScioIOTest
import com.spotify.scio.avro._
import com.spotify.scio.io.{ClosedTap, FileNamePolicySpec}
import com.spotify.scio.proto.Track.TrackPB
import com.spotify.scio.testing.ScioIOSpec
import com.spotify.scio.util.FilenamePolicySupplier
import com.spotify.scio.values.SCollection
import org.apache.avro.generic.GenericRecord

import java.io.File

object AvroIOTest {
  @AvroType.toSchema
  case class AvroRecord(i: Int, s: String, r: List[String])
}

class AvroIOFileNamePolicyTest extends FileNamePolicySpec[TestRecord] {
  import ScioIOTest._
  override def suffix: String = ".avro"
  override def save(
    filenamePolicySupplier: FilenamePolicySupplier = null,
    prefix: String = null,
    shardNameTemplate: String = null
  )(in: SCollection[Int], tmpDir: String, isBounded: Boolean): ClosedTap[TestRecord] = {
    in.map(AvroUtils.newSpecificRecord)
      .saveAsAvroFile(
        tmpDir,
        // TODO there is an exception with auto-sharding that fails for unbounded streams due to a GBK so numShards must be specified
        numShards = if (isBounded) 0 else TestNumShards,
        filenamePolicySupplier = filenamePolicySupplier,
        prefix = prefix,
        shardNameTemplate = shardNameTemplate
      )
  }

  override def failSaves: Seq[SCollection[Int] => ClosedTap[TestRecord]] = Seq(
    _.map(AvroUtils.newSpecificRecord).saveAsAvroFile(
      "nonsense",
      shardNameTemplate = "SSS-of-NNN",
      filenamePolicySupplier = testFilenamePolicySupplier
    )
  )
}

class ObjectIOFileNamePolicyTest extends FileNamePolicySpec[AvroIOTest.AvroRecord] {
  import ScioIOTest._
  import AvroIOTest._

  override def suffix: String = ".obj.avro"
  override def save(
    filenamePolicySupplier: FilenamePolicySupplier = null,
    prefix: String = null,
    shardNameTemplate: String = null
  )(in: SCollection[Int], tmpDir: String, isBounded: Boolean): ClosedTap[AvroRecord] = {
    in.map(x => AvroRecord(x, x.toString, (1 to x).map(_.toString).toList))
      .saveAsObjectFile(
        tmpDir,
        // TODO there is an exception with auto-sharding that fails for unbounded streams due to a GBK so numShards must be specified
        numShards = if (isBounded) 0 else TestNumShards,
        filenamePolicySupplier = filenamePolicySupplier,
        prefix = prefix,
        shardNameTemplate = shardNameTemplate
      )
  }

  override def failSaves: Seq[SCollection[Int] => ClosedTap[AvroRecord]] = Seq(
    _.map(x => AvroRecord(x, x.toString, (1 to x).map(_.toString).toList)).saveAsObjectFile(
      "nonsense",
      shardNameTemplate = "SSS-of-NNN",
      filenamePolicySupplier = testFilenamePolicySupplier
    )
  )
}

class ProtobufIOFileNamePolicyTest extends FileNamePolicySpec[TrackPB] {
  import ScioIOTest._
  override def suffix: String = ".protobuf.avro"
  override def save(
    filenamePolicySupplier: FilenamePolicySupplier = null,
    prefix: String = null,
    shardNameTemplate: String = null
  )(in: SCollection[Int], tmpDir: String, isBounded: Boolean): ClosedTap[TrackPB] = {
    in.map(x => TrackPB.newBuilder().setTrackId(x.toString).build())
      .saveAsProtobufFile(
        tmpDir,
        // TODO there is an exception with auto-sharding that fails for unbounded streams due to a GBK so numShards must be specified
        numShards = if (isBounded) 0 else TestNumShards,
        filenamePolicySupplier = filenamePolicySupplier,
        prefix = prefix,
        shardNameTemplate = shardNameTemplate
      )
  }

  override def failSaves: Seq[SCollection[Int] => ClosedTap[TrackPB]] = Seq(
    _.map(x => TrackPB.newBuilder().setTrackId(x.toString).build()).saveAsProtobufFile(
      "nonsense",
      shardNameTemplate = "SSS-of-NNN",
      filenamePolicySupplier = testFilenamePolicySupplier
    )
  )
}

class AvroIOTest extends ScioIOSpec {
  import ScioIOTest._
  import AvroIOTest._
  def relativeFiles(dir: File): Seq[String] =
    listFiles(dir).map(_.toPath.relativize(dir.toPath)).sorted.map(_.toString)

  "AvroIO" should "work with SpecificRecord" in {
    val xs = (1 to 100).map(AvroUtils.newSpecificRecord)
    testTap(xs)(_.saveAsAvroFile(_))(".avro")
    testJobTest(xs)(AvroIO[TestRecord](_))(_.avroFile(_))(_.saveAsAvroFile(_))
  }

  it should "work with GenericRecord" in {
    import AvroUtils.schema
    implicit val coder = avroGenericRecordCoder(schema)
    val xs = (1 to 100).map(AvroUtils.newGenericRecord)
    testTap(xs)(_.saveAsAvroFile(_, schema = schema))(".avro")
    testJobTest(xs)(AvroIO(_))(_.avroFile(_, schema))(_.saveAsAvroFile(_, schema = schema))
  }

  it should "work with typed Avro" in {
    val xs = (1 to 100).map(x => AvroRecord(x, x.toString, (1 to x).map(_.toString).toList))
    val io = (s: String) => AvroIO[AvroRecord](s)
    testTap(xs)(_.saveAsTypedAvroFile(_))(".avro")
    testJobTest(xs)(io)(_.typedAvroFile[AvroRecord](_))(_.saveAsTypedAvroFile(_))
  }

  it should "work with GenericRecord and a parseFn" in {
    import AvroUtils.schema
    implicit val coder = avroGenericRecordCoder(schema)
    val xs = (1 to 100).map(AvroUtils.newGenericRecord)
    // No test for saveAsAvroFile because parseFn is only for input
    testJobTest(xs)(AvroIO(_))(
      _.parseAvroFile[GenericRecord](_)(identity)
    )(_.saveAsAvroFile(_, schema = schema))
  }

  "ObjectFileIO" should "work" in {
    val xs = (1 to 100).map(x => AvroRecord(x, x.toString, (1 to x).map(_.toString).toList))
    testTap(xs)(_.saveAsObjectFile(_))(".obj.avro")
    testJobTest[AvroRecord](xs)(ObjectFileIO[AvroRecord](_))(_.objectFile[AvroRecord](_))(
      _.saveAsObjectFile(_)
    )
  }

  "ProtobufIO" should "work" in {
    val xs =
      (1 to 100).map(x => TrackPB.newBuilder().setTrackId(x.toString).build())
    val suffix = ".protobuf.avro"
    testTap(xs)(_.saveAsProtobufFile(_))(suffix)
    testJobTest(xs)(ProtobufIO(_))(_.protobufFile[TrackPB](_))(_.saveAsProtobufFile(_))
  }

}