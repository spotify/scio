package com.spotify.scio.io

import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.io.{AccessibleBeam, ReadAllViaFileBasedSourceWithFilename, TextSource}
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider
import org.apache.beam.sdk.{io => beam}
import org.apache.commons.io.FileUtils

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

class FoobarTest extends PipelineSpec {

  it should "work with text" in {
    val temp = Files.createTempDirectory("filename-retaining")
    temp.toFile.deleteOnExit()
    val file = new File(temp.toFile, "input.txt")
    val fileContents =
      """
        |line1
        |line2
        |line3
        |""".stripMargin
    FileUtils.write(file, fileContents, StandardCharsets.UTF_8)

    runWithContext { sc =>
      sc.parallelize(List(file.toString))
        .applyTransform(beam.FileIO.matchAll())
        .applyTransform(beam.FileIO.readMatches())
        .applyTransform(
          new ReadAllViaFileBasedSourceWithFilename[String](
            64 * 1024 * 1024L,
            AccessibleBeam.textSource,
          )
        )
        .debug()
    }
  }

}
