package com.spotify.scio.examples.extra
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.avro._
import com.spotify.scio.avro.types.AvroType
// Example: Read multiple files/file-patterns
// Usage:

// `sbt "runMain com.spotify.scio.examples.extra.AvroReadFilesExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --inputs=[INPUT_1].avro,[INPUT_2].avro --output=[OUTPUT].avro"`
object AvroReadFilesExample {

  @AvroType.toSchema
  case class Raw(text: String)
  final case class WordCount(word: String, count: Int)

  def main(cmdlineArgs: Array[String]): Unit = {
//  Create `ScioContext` and `Args`
    val (sc, args) = ContextAndArgs(cmdlineArgs)

//  pass `--inputs=<file_1>,<file_2>,....` or `--inputs=<file_pattern_1>,<file_pattern_2>`
//  i.e  `--inputs=first.avro,second.avro`
//  Also support any combination of files and patterns
    val inputs = args.list("inputs")

//  Read avro data from multiple files or file-patterns
    sc.typedAvroFiles[Raw](inputs)
      .flatMap(r => r.text.split("\\s"))
      .map(w => (w, 1))
      .sumByKey
      .map(WordCount.tupled)
      .saveAsTextFile(args("output"))

    sc.run()
    ()
  }

}
