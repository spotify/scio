package com.spotify.scio.transforms

import com.spotify.scio.coders.{Coder, CoderMaterializer}
import org.apache.beam.sdk.io.FileIO.ReadableFile
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.options.ValueProvider
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms._
import org.apache.beam.sdk.values.{KV, PCollection}
import org.apache.beam.sdk.{io => beam}

import java.lang.reflect.{Constructor, Method}
import scala.util.Using.Manager

object AccessibleBeam {
  private def yolo(className: String, args: Class[_]*): Constructor[_] = {
    val clazz = Class.forName(className)
    val ctor: Constructor[_] = clazz.getDeclaredConstructor(args: _*)
    ctor.setAccessible(true)
    ctor
  }

  private def yoloMethod(className: String, methodName: String, args: Class[_]*) = {
    val clazz = Class.forName(className)
    val method = clazz.getDeclaredMethod(methodName, args: _*)
    method.setAccessible(true)
    method
  }

  @transient private lazy val TextSourceCtor = yolo(
    // TODO harass beam to allow access to this
    "org.apache.beam.sdk.io.TextSource",
    classOf[ValueProvider[String]],
    classOf[EmptyMatchTreatment],
    classOf[Array[Byte]]
  )

  def textSource(input: String): beam.FileBasedSource[String] = {
    TextSourceCtor
      .newInstance(
        ValueProvider.StaticValueProvider.of(input),
        EmptyMatchTreatment.DISALLOW,
        Array[Byte]('\n')
      )
      .asInstanceOf[beam.FileBasedSource[String]]
  }

  // TODO harass beam to allow access to this
  @transient private lazy val SourceSubrangeMethod: Method =
    AccessibleBeam.yoloMethod(
      "org.apache.beam.sdk.io.FileBasedSource",
      "createForSubrangeOfFile",
      classOf[beam.fs.MatchResult.Metadata],
      classOf[Long],
      classOf[Long]
    )

  def sourceSubrange[T](
    source: beam.FileBasedSource[T],
    metadata: beam.fs.MatchResult.Metadata,
    from: Long,
    to: Long
  ): beam.FileBasedSource[T] = {
    // source.createForSubrangeOfFile(file.getMetadata, range.getFrom, range.getTo)
    SourceSubrangeMethod
      .invoke(source, metadata, from, to)
      .asInstanceOf[beam.FileBasedSource[T]]
  }

  @transient private lazy val SplitIntoRnagesFnCtor = yolo(
    // TODO harass beam to allow access to this
    "org.apache.beam.sdk.io.ReadAllViaFileBasedSource$SplitIntoRangesFn",
    classOf[Long]
  )

  type SplitIntoRangesT = DoFn[beam.FileIO.ReadableFile, KV[beam.FileIO.ReadableFile, OffsetRange]]
  def splitIntoRangesFn(desiredBundleSizeBytes: Long): SplitIntoRangesT =
    SplitIntoRnagesFnCtor.newInstance(desiredBundleSizeBytes).asInstanceOf[SplitIntoRangesT]
}

object ReadAllViaFileBasedSourceWithFilename {
  val DefaultReadFileRangesFnExceptionHandler =
    new beam.ReadAllViaFileBasedSource.ReadFileRangesFnExceptionHandler
}

/**
 * Exactly the same as [[beam.ReadAllViaFileBasedSource]] except uses
 * [[FilenameRetainingReadFileRangesFn]] instead of
 * [[beam.ReadAllViaFileBasedSource.ReadFileRangesFn]]
 */
class ReadAllViaFileBasedSourceWithFilename[T](
  createSource: SerializableFunction[String, _ <: beam.FileBasedSource[T]],
  desiredBundleSizeBytes: Long = (64 * 1024 * 1024L), // 64 mb
  usesReshuffle: Boolean = true,
  exceptionHandler: beam.ReadAllViaFileBasedSource.ReadFileRangesFnExceptionHandler =
    ReadAllViaFileBasedSourceWithFilename.DefaultReadFileRangesFnExceptionHandler
)(implicit
  coder: Coder[T]
) extends PTransform[PCollection[ReadableFile], PCollection[(String, T)]] {

  private val outCoder = CoderMaterializer
    .beamWithDefault(Coder.tuple2Coder[String, T](Coder.stringCoder, coder))

  override def expand(input: PCollection[ReadableFile]): PCollection[(String, T)] = {
    var ranges: PCollection[KV[ReadableFile, OffsetRange]] = input.apply(
      "Split into ranges",
      ParDo.of(AccessibleBeam.splitIntoRangesFn(desiredBundleSizeBytes))
    )
    if (usesReshuffle) {
      ranges = ranges.apply("Reshuffle", Reshuffle.viaRandomKey[KV[ReadableFile, OffsetRange]])
    }
    ranges
      .apply(
        "Read ranges with filename",
        ParDo.of(new FilenameRetainingReadFileRangesFn[T](createSource, exceptionHandler))
      )
      .setCoder(outCoder)
  }
}

/**
 * Exactly the same as [[beam.ReadAllViaFileBasedSource.ReadFileRangesFn]] except it outputs the
 * filename in addition to the data contents.
 */
class FilenameRetainingReadFileRangesFn[T](
  createSource: SerializableFunction[String, _ <: beam.FileBasedSource[T]],
  exceptionHandler: beam.ReadAllViaFileBasedSource.ReadFileRangesFnExceptionHandler
) extends DoFn[KV[beam.FileIO.ReadableFile, OffsetRange], (String, T)] {
  type DoFnT = DoFn[KV[beam.FileIO.ReadableFile, OffsetRange], (String, T)]

  @ProcessElement
  def process(c: DoFnT#ProcessContext): Unit = {
    val file: beam.FileIO.ReadableFile = c.element.getKey
    val range: OffsetRange = c.element.getValue

    val filename = file.getMetadata.resourceId.toString
    val source: beam.FileBasedSource[T] = beam.CompressedSource
      .from(createSource.apply(filename))
      .withCompression(file.getCompression)

    Manager { use =>
      val reader: beam.BoundedSource.BoundedReader[T] = use(
        // source.createForSubrangeOfFile(file.getMetadata, range.getFrom, range.getTo)
        AccessibleBeam
          .sourceSubrange(source, file.getMetadata, range.getFrom, range.getTo)
          .createReader(c.getPipelineOptions)
      )

      try {
        var more = reader.start();
        while (more) {
          // this is the only salient change
          c.output((filename, reader.getCurrent))
          more = reader.advance()
        }
      } catch {
        case e: RuntimeException =>
          if (exceptionHandler.apply(file, range, e)) {
            throw e
          }
      }
    }
  }
}
