package com.spotify.scio.io

import com.google.common.collect.{ImmutableList, ImmutableMap}
import com.spotify.scio.testing.PipelineSpec
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.apache.beam.runners.direct.DirectRunner
import org.apache.beam.sdk.io.{AccessibleBeam, ReadAllViaFileBasedSourceWithFilename}
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.apache.commons.io.FileUtils

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

class FoobarTest extends PipelineSpec {

  def localOptions: PipelineOptions = {
    val options = PipelineOptionsFactory.create()
    options.setRunner(classOf[DirectRunner])
    options
  }

  def withTempDir(prefix: String)(fn: Path => Unit): Unit = {
    val temp = Files.createTempDirectory(prefix)
    temp.toFile.deleteOnExit()
    fn(temp)
  }

  it should "work with text" in {
    withTempDir("filename-retaining-text") { temp =>
      runWithRealContext(localOptions) { sc =>
        sc
          .parallelize(1 to 10)
          .map { i => s"line$i" }
          .saveAsTextFile(temp.toString)
      }

      runWithRealContext(localOptions) { sc =>
        sc.parallelize(List(s"${temp.toString}/*.txt"))
          .readFiles(new ReadAllViaFileBasedSourceWithFilename[String](AccessibleBeam.textSource))
          .debug()
      }
    }
  }

  it should "work with avro" in {
    withTempDir("filename-retaining-avro") { temp =>
      import com.spotify.scio.avro._

      runWithRealContext(localOptions) { sc =>
        sc
          .parallelize(1 to 10)
          .map { i => StringFieldTest.newBuilder().setStrField(s"someStr$i").build() }
          .saveAsAvroFile(temp.toString)
      }

      // specific records
      runWithRealContext(localOptions) { sc =>
        sc.parallelize(List(s"${temp.toString}/*.avro"))
          .readFiles(
            new ReadAllViaFileBasedSourceWithFilename[StringFieldTest](
              AccessibleBeam.avroSource[StringFieldTest]
            )
          )
          .mapValues(_.getStrField)
          .debug()
      }

      // generic records
      runWithRealContext(localOptions) { sc =>
        sc.parallelize(List(s"${temp.toString}/*.avro"))
          .readFiles(
            new ReadAllViaFileBasedSourceWithFilename[GenericRecord](
              AccessibleBeam.avroGenericSource(_, StringFieldTest.SCHEMA$)
            )
          )
          .mapValues(_.get("strField").asInstanceOf[CharSequence].toString)
          .debug()
      }

    }
  }
}
