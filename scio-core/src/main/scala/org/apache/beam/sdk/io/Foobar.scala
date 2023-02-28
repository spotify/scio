package org.apache.beam.sdk.io

import com.spotify.scio.coders.{Coder, CoderMaterializer}
import org.apache.beam.sdk.io.FileIO.ReadableFile
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.options.ValueProvider
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms._
import org.apache.beam.sdk.values.{KV, PCollection}
import org.apache.beam.sdk.{io => beam}

import java.lang.reflect.Constructor
import scala.collection.JavaConverters._
import scala.util.Using.Manager


object AccessibleBeam {
  private def yolo(className: String, args: Class[_]*): Constructor[_] = {
    val clazz = Class.forName(className)
    val ctor: Constructor[_] = clazz.getDeclaredConstructor(args:_*)
    ctor.setAccessible(true)
    ctor
  }

  def textSource(input: String): FileBasedSource[String] = {
    yolo(
      "org.apache.beam.sdk.io.TextSource",
      classOf[ValueProvider[String]], classOf[EmptyMatchTreatment], classOf[Array[Byte]]
    ).newInstance(
      ValueProvider.StaticValueProvider.of(input),
      EmptyMatchTreatment.DISALLOW,
      Array[Byte]('\n')
    ).asInstanceOf[FileBasedSource[String]]
//    val txtSrcClazz = Class.forName("org.apache.beam.sdk.io.TextSource")
//    val txtSrcDefaultCtor = txtSrcClazz.getDeclaredConstructor(
//      classOf[ValueProvider[String]], classOf[EmptyMatchTreatment], classOf[Array[Byte]]
//    )
//    txtSrcDefaultCtor.setAccessible(true)
//    txtSrcDefaultCtor.newInstance(
//      ValueProvider.StaticValueProvider.of(input), EmptyMatchTreatment.DISALLOW, Array[Byte]('\n')
//    ).asInstanceOf[FileBasedSource[String]]
  }

  type SplitIntoRangesT = DoFn[beam.FileIO.ReadableFile, KV[beam.FileIO.ReadableFile, OffsetRange]]
  def splitIntoRangesFn(desiredBundleSizeBytes: Long): SplitIntoRangesT = {
    yolo(
      "org.apache.beam.sdk.io.ReadAllViaFileBasedSource$SplitIntoRangesFn",
      classOf[Long]
    ).newInstance(
      desiredBundleSizeBytes
    ).asInstanceOf[SplitIntoRangesT]
  }
}

object ReadAllViaFileBasedSourceWithFilename {
  val DefaultReadFileRangesFnExceptionHandler = new ReadAllViaFileBasedSource.ReadFileRangesFnExceptionHandler
}

class ReadAllViaFileBasedSourceWithFilename[T](
  desiredBundleSizeBytes: Long,
  createSource: SerializableFunction[String, _ <: FileBasedSource[T]],
  usesReshuffle: Boolean = true,
  exceptionHandler: ReadAllViaFileBasedSource.ReadFileRangesFnExceptionHandler = ReadAllViaFileBasedSourceWithFilename.DefaultReadFileRangesFnExceptionHandler
)(implicit
  coder: Coder[T]
) extends PTransform[PCollection[ReadableFile], PCollection[
  (String, T)]] {

  private val outCoder = CoderMaterializer
    .beamWithDefault(Coder.tuple2Coder[String, T](Coder.stringCoder, coder))

  override def expand(input: PCollection[ReadableFile]): PCollection[(String, T)] = {
    var ranges: PCollection[KV[ReadableFile, OffsetRange]] = input.apply(
      "Split into ranges",
      ParDo.of(AccessibleBeam.splitIntoRangesFn(desiredBundleSizeBytes))
    )
    if (usesReshuffle)
      ranges = ranges.apply(
        "Reshuffle",
        Reshuffle.viaRandomKey[KV[ReadableFile, OffsetRange]]
      )

    ranges.apply(
      "Read ranges with filename",
      ParDo.of(new FilenameRetainingReadFileRangesFn[T](createSource, exceptionHandler)))
      .setCoder(outCoder)
  }
}

class FilenameRetainingReadFileRangesFn[T] (
  createSource: SerializableFunction[String, _ <: FileBasedSource[T]],
  exceptionHandler: ReadAllViaFileBasedSource.ReadFileRangesFnExceptionHandler
)
  extends DoFn[KV[beam.FileIO.ReadableFile, OffsetRange], (String, T)] {
  type DoFnT = DoFn[KV[beam.FileIO.ReadableFile, OffsetRange], (String, T)]

  @ProcessElement
  def process(c: DoFnT#ProcessContext): Unit = {
    val file: beam.FileIO.ReadableFile = c.element.getKey
    val range: OffsetRange = c.element.getValue

    val filename = file.getMetadata.resourceId.toString
    val source: FileBasedSource[T] = beam.CompressedSource
      .from(createSource.apply(filename))
      .withCompression(file.getCompression)

    Manager { use =>
      val reader: beam.BoundedSource.BoundedReader[T] = use(
        source
          .createForSubrangeOfFile(file.getMetadata, range.getFrom, range.getTo)
          .createReader(c.getPipelineOptions)
      )

      try {
        var more = reader.start();
        while (more) {
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

//
//import org.apache.beam.sdk.coders.{Coder => BCoder}
//import org.apache.beam.sdk.io.FileBasedSource.FileBasedReader
//import org.apache.beam.sdk.io.fs.MatchResult
//import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider
//import org.apache.beam.sdk.options.PipelineOptions
//import java.nio.channels.ReadableByteChannel
//
//case class FilenameRetainingTextBasedReader(
//  fn: String,
//  src: beam.FileBasedSource[(String, String)],
//  underlyingReader: FileBasedReader[String]
//) extends FileBasedReader[(String, String)](src) {
//  private val clazz = underlyingReader.getClass
//  private val methods = Map(
//    "startReading" -> clazz.getDeclaredMethod("startReading", classOf[ReadableByteChannel])
//  ) ++ List("readNextRecord", "getCurrentOffset", "getCurrent")
//    .map { m => m -> clazz.getDeclaredMethod(m) }
//    .toMap
//
//  methods.foreach { case (_, v) => v.setAccessible(true) }
//
//  override def startReading(channel: ReadableByteChannel): Unit = {
//    //    underlyingReader.startReading(channel)
//    methods("startReading").invoke(underlyingReader, channel)
//  }
//
//  override def readNextRecord(): Boolean = {
//    //    underlyingReader.readNextRecord()
//    methods("readNextRecord").invoke(underlyingReader).asInstanceOf[Boolean]
//  }
//  override def getCurrentOffset: Long = {
////    underlyingReader.getCurrentOffset
//    methods("getCurrentOffset").invoke(underlyingReader).asInstanceOf[Long]
//  }
//  override def getCurrent: (String, String) = {
////    (fn, underlyingReader.getCurrent)
//    val res = methods("getCurrent").invoke(underlyingReader).asInstanceOf[String]
//    (fn, res)
//  }
//}
//
//trait Common {
//  protected val outputCoder: BCoder[(String, String)] =
//    CoderMaterializer.beamWithDefault(
//      Coder.tuple2Coder[String, String](Coder.stringCoder, Coder.stringCoder)
//    )
//
//  val txtSrcClazz = Class.forName("org.apache.beam.sdk.io.TextSource")
//  val txtSrcDefaultCtor = txtSrcClazz.getDeclaredConstructor(
//    classOf[ValueProvider[String]], classOf[EmptyMatchTreatment], classOf[Array[Byte]]
//  )
//  txtSrcDefaultCtor.setAccessible(true)
//  val txtSrcMetadataCtor = txtSrcClazz.getDeclaredConstructor(
//    classOf[MatchResult.Metadata], classOf[Long], classOf[Long], classOf[Array[Byte]]
//  )
//  txtSrcMetadataCtor.setAccessible(true)
//  val readerClazz = Class.forName("org.apache.beam.sdk.io.TextSource$TextBasedReader")
//  val readerCtor = readerClazz.getDeclaredConstructor(txtSrcClazz, classOf[Array[Byte]])
//  readerCtor.setAccessible(true)
//}
//
//case class SplitFilenameRetainingTextSource(
//  metadata: MatchResult.Metadata,
//  delimiter: Array[Byte],
//  start: Long,
//  end: Long
//) extends beam.FileBasedSource[(String, String)](metadata, 1L, start, end)
//    with Common {
//  override def getOutputCoder: BCoder[(String, String)] = outputCoder
//
//  def createForSubrangeOfFile(
//    fileMetadata: MatchResult.Metadata,
//    start: Long,
//    end: Long
//  ): beam.FileBasedSource[(String, String)] =
//    SplitFilenameRetainingTextSource(fileMetadata, delimiter, start, end)
//
//  override def createSingleFileReader(
//    options: PipelineOptions
//  ): beam.FileBasedSource.FileBasedReader[(String, String)] = {
//    val underlyingReader = readerCtor
//      .newInstance(txtSrcMetadataCtor.newInstance(metadata, start, end, delimiter), delimiter)
//      .asInstanceOf[FileBasedReader[String]]
//    val fn = metadata.resourceId().toString
//    FilenameRetainingTextBasedReader(fn, this, underlyingReader)
//  }
//
//}
//
//case class FilenameRetainingTextSource(
//  fileSpec: ValueProvider[String],
//  emptyMatchTreatment: EmptyMatchTreatment,
//  delimiter: Array[Byte]
//) extends beam.FileBasedSource[(String, String)](fileSpec, emptyMatchTreatment, 1L)
//    with Common {
//  override def getOutputCoder: BCoder[(String, String)] = outputCoder
//
//  def createForSubrangeOfFile(
//    fileMetadata: MatchResult.Metadata,
//    start: Long,
//    end: Long
//  ): beam.FileBasedSource[(String, String)] =
//    SplitFilenameRetainingTextSource(fileMetadata, delimiter, start, end)
//
//  override def createSingleFileReader(
//    options: PipelineOptions
//  ): FileBasedReader[(String, String)] = {
//    val fn = fileSpec.get()
//    val underlyingReader = readerCtor
//      .newInstance(
//        txtSrcDefaultCtor.newInstance(StaticValueProvider.of(fn), EmptyMatchTreatment.DISALLOW, delimiter),
//        delimiter
//      )
//      .asInstanceOf[FileBasedReader[String]]
//    FilenameRetainingTextBasedReader(fn, this, underlyingReader)
//  }
//}
