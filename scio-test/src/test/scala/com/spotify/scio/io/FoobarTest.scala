package com.spotify.scio.io

import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.io.{AccessibleBeam, ReadAllViaFileBasedSourceWithFilename, TextSource}
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.{io => beam}
import org.apache.commons.io.FileUtils

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

class FoobarTest extends PipelineSpec {

  def withTempDir(prefix: String)(fn: Path => Unit): Unit = {
    val temp = Files.createTempDirectory(prefix)
    temp.toFile.deleteOnExit()
    fn(temp)
  }

  def textFile(path: Path, name: String, contents: String): File = {
    val file = new File(path.toFile, name)
    FileUtils.write(file, contents, StandardCharsets.UTF_8)
    file
  }

  it should "work with text" in {
    withTempDir("filename-retaining-text") { temp =>
      val file1 = textFile(temp, "input1.txt", "line1\nline2\nline3\n")
      val file2 = textFile(temp, "input2.txt", "line4\nline5\nline6\n")

      runWithContext { sc =>
        sc.parallelize(List(file1.toString, file2.toString))
          .readFiles(
            new ReadAllViaFileBasedSourceWithFilename[String](
              64 * 1024 * 1024L,
              AccessibleBeam.textSource,
            )
          )
          .debug()
      }
    }
  }

  it should "work with avro" in {
    withTempDir("filename-retaining-avro") { temp =>

    }
  }
}
